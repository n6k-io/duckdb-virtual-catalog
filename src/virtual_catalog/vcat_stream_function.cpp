#include "vcat_stream_function.hpp"

#include "arrow_frame_decoder.hpp"
#include "sql_escape.hpp"
#include "yyjson_util.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "nanoarrow.h"
#include "yyjson.hpp"

#include <atomic>
#include <cerrno>
#include <string>

namespace duckdb {
namespace vcat {

namespace {

// The host parses these itself, so they go as JSON rather than being rendered into SQL. Built with
// yyjson rather than by hand so string escaping is not ours to get wrong.
std::string ArgsJsonFromValues(const vector<Value> &values) {
	using namespace duckdb_yyjson; // NOLINT
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *arr = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, arr);
	for (auto &value : values) {
		if (value.IsNull()) {
			yyjson_mut_arr_add_null(doc, arr);
			continue;
		}
		switch (value.type().id()) {
		case LogicalTypeId::BOOLEAN:
			yyjson_mut_arr_add_bool(doc, arr, value.GetValue<bool>());
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
			yyjson_mut_arr_add_sint(doc, arr, value.GetValue<int64_t>());
			break;
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			yyjson_mut_arr_add_real(doc, arr, value.GetValue<double>());
			break;
		default: {
			// Dates, decimals and 128-bit ints have no JSON form; they cross as text, which is the
			// same shape the wire path already sends (RpcArgsJson).
			auto text = value.ToString();
			yyjson_mut_arr_add_strncpy(doc, arr, text.c_str(), text.size());
			break;
		}
		}
	}
	return SerializeJsonDocAndFree(doc);
}

// An ArrowArrayStream backed by the host's next/close UDFs. One generator; releasing the stream
// closes it, which is what runs the host's own `finally`.
struct HostStream {
	unique_ptr<Connection> conn; // outlives next_stmt, which borrows it
	unique_ptr<PreparedStatement> next_stmt;
	std::string close_udf;
	std::string handle;
	ArrowFrameDecoder decoder;
	ArrowSchema schema {};
	bool schema_owned = false;
	bool opened = false;
	bool exhausted = false;
	bool closed = false;
	std::string last_error;

	~HostStream() {
		CloseOnce();
		if (schema_owned && schema.release) {
			schema.release(&schema);
		}
	}

	void CloseOnce() {
		if (!opened || closed || !conn) {
			return;
		}
		closed = true;
		try {
			// The handle is interpolated, not bound: it is generated below and never client-supplied.
			conn->Query("SELECT " + QuoteIdent(close_udf) + "('" + handle + "')");
		} catch (...) { // NOLINT: a failing close must not displace the error already in flight
		}
	}
};

int HostStreamGetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
	auto *self = static_cast<HostStream *>(stream->private_data);
	if (!self->schema_owned) {
		self->last_error = "virtual_catalog: stream has no schema";
		return EINVAL;
	}
	return ArrowSchemaDeepCopy(&self->schema, out) == NANOARROW_OK ? 0 : EIO;
}

int HostStreamGetNext(ArrowArrayStream *stream, ArrowArray *out) {
	auto *self = static_cast<HostStream *>(stream->private_data);
	out->release = nullptr;
	if (self->exhausted) {
		return 0;
	}
	for (;;) {
		if (self->decoder.HasBatch()) {
			return self->decoder.TryPopBatch(out) ? 0 : EIO;
		}
		vector<Value> params;
		params.emplace_back(self->handle);
		unique_ptr<QueryResult> result;
		try {
			result = self->next_stmt->Execute(params);
		} catch (const std::exception &e) {
			self->last_error = e.what();
			return EIO;
		}
		if (result->HasError()) {
			self->last_error = result->GetError();
			return EIO;
		}
		auto chunk = result->Fetch();
		// NULL is how the host says the generator is exhausted; there is no separate end signal.
		if (!chunk || chunk->size() == 0 || chunk->GetValue(0, 0).IsNull()) {
			self->exhausted = true;
			self->CloseOnce();
			return 0;
		}
		auto batch_value = chunk->GetValue(0, 0);
		if (!self->decoder.PushFrame(StringValue::Get(batch_value))) {
			self->last_error = self->decoder.ErrorMessage();
			return EIO;
		}
		// A yield that produced no rows decodes to no batch; pull again rather than reporting EOS.
		if (!self->decoder.HasBatch() && self->decoder.SawEndOfStream()) {
			self->exhausted = true;
			self->CloseOnce();
			return 0;
		}
	}
}

const char *HostStreamGetLastError(ArrowArrayStream *stream) {
	auto *self = static_cast<HostStream *>(stream->private_data);
	return self->last_error.empty() ? nullptr : self->last_error.c_str();
}

void HostStreamRelease(ArrowArrayStream *stream) {
	delete static_cast<HostStream *>(stream->private_data);
	stream->private_data = nullptr;
	stream->get_schema = nullptr;
	stream->get_next = nullptr;
	stream->get_last_error = nullptr;
	stream->release = nullptr;
}

// Runs the host's open UDF and leaves `out` ready to read. The generator is live once open returns a
// schema, so every failure path past that point closes it.
void OpenHostStream(ClientContext &context, const StreamFunctionInfo &info, const std::string &args_json,
                    ArrowArrayStream *out) {
	// Only ever appears inside a name this extension generated, so a counter keeps concurrent binds
	// apart. Distinct prefix from the reactor's own handles so the two cannot collide on a
	// connection serving both.
	static std::atomic<uint64_t> handle_counter {0};
	auto self = make_uniq<HostStream>();
	self->handle = "b" + std::to_string(handle_counter.fetch_add(1) + 1);
	self->close_udf = info.close_udf;
	self->conn = make_uniq<Connection>(*context.db);

	auto open_stmt = self->conn->Prepare("SELECT " + QuoteIdent(info.open_udf) + "(?, ?, ?, ?)");
	if (open_stmt->HasError()) {
		open_stmt->GetErrorObject().Throw("virtual_catalog: stream open function \"" + info.open_udf +
		                                  "\" is unusable: ");
	}
	// Bound, never interpolated: args_json carries the caller's values verbatim.
	vector<Value> open_params;
	open_params.emplace_back(self->handle);
	open_params.emplace_back(info.function_name);
	open_params.emplace_back(args_json);
	open_params.push_back(Value(LogicalType::BLOB)); // no pushed rows on the local path
	auto open_result = open_stmt->Execute(open_params);
	if (open_result->HasError()) {
		open_result->ThrowError();
	}
	auto open_chunk = open_result->Fetch();
	if (!open_chunk || open_chunk->size() == 0) {
		throw IOException("virtual_catalog: stream \"%s\" returned no Arrow schema when opened", info.function_name);
	}
	// Held in a named Value: StringValue::Get borrows from it.
	const Value schema_value = open_chunk->GetValue(0, 0);
	if (schema_value.IsNull()) {
		throw IOException("virtual_catalog: stream \"%s\" returned no Arrow schema when opened", info.function_name);
	}
	self->opened = true;

	const auto &schema_message = StringValue::Get(schema_value);
	if (schema_message.empty()) {
		throw IOException("virtual_catalog: stream \"%s\" returned an empty Arrow schema when opened",
		                  info.function_name);
	}
	if (!self->decoder.PushFrame(schema_message) || !self->decoder.HasSchema()) {
		throw IOException("virtual_catalog: stream \"%s\" returned an unreadable Arrow schema: %s", info.function_name,
		                  self->decoder.ErrorMessage());
	}
	if (ArrowSchemaDeepCopy(self->decoder.GetSchema(), &self->schema) != NANOARROW_OK) {
		throw IOException("virtual_catalog: stream \"%s\": ArrowSchemaDeepCopy failed", info.function_name);
	}
	self->schema_owned = true;

	self->next_stmt = self->conn->Prepare("SELECT " + QuoteIdent(info.next_udf) + "(?)");
	if (self->next_stmt->HasError()) {
		self->next_stmt->GetErrorObject().Throw("virtual_catalog: stream next function \"" + info.next_udf +
		                                        "\" is unusable: ");
	}

	out->get_schema = HostStreamGetSchema;
	out->get_next = HostStreamGetNext;
	out->get_last_error = HostStreamGetLastError;
	out->release = HostStreamRelease;
	out->private_data = self.release();
}

struct HostStreamData {
	ArrowArrayStream stream;
	bool consumed = false;

	HostStreamData() {
		stream.release = nullptr;
	}
	~HostStreamData() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

struct StreamScanData : public ArrowScanFunctionData {
	unique_ptr<HostStreamData> owned_stream_data;

	StreamScanData(stream_factory_produce_t producer, unique_ptr<HostStreamData> stream_data)
	    : ArrowScanFunctionData(producer, reinterpret_cast<uintptr_t>(stream_data.get())),
	      owned_stream_data(std::move(stream_data)) {
		// The generator is single-pass, so a projected re-scan cannot be honoured.
		projection_pushdown_enabled = false;
	}
};

unique_ptr<ArrowArrayStreamWrapper> TakeHostStreamOnce(uintptr_t factory_ptr, ArrowStreamParameters &) {
	auto *stream_data = reinterpret_cast<HostStreamData *>(factory_ptr);
	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	if (stream_data->consumed) {
		wrapper->arrow_array_stream.release = nullptr;
		return wrapper;
	}
	wrapper->arrow_array_stream = stream_data->stream;
	stream_data->stream.release = nullptr;
	stream_data->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

// Opening at bind time is what makes the schema knowable: the host declares it in `open`, and the
// binder needs it before any row is read. Its address is also the stream's identity, matched against
// by vcat_stream_functions.
unique_ptr<FunctionData> OpenStreamAndBindSchema(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<StreamFunctionInfo>();

	auto stream_data = make_uniq<HostStreamData>();
	OpenHostStream(context, info, ArgsJsonFromValues(input.inputs), &stream_data->stream);

	auto result = make_uniq<StreamScanData>(reinterpret_cast<stream_factory_produce_t>(TakeHostStreamOnce),
	                                        std::move(stream_data));
	auto *sp = result->owned_stream_data.get();
	sp->stream.get_schema(&sp->stream, &result->schema_root.arrow_schema);
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	return std::move(result);
}

std::string ArgString(DataChunk &args, idx_t col, idx_t row) {
	return FlatVector::GetData<string_t>(args.data[col])[row].GetString();
}

// vcat_create_stream_function(catalog, schema, name, open_udf, next_udf, close_udf) -> 'ok'
void CreateStreamFunctionEntries(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	const auto count = args.size();
	for (idx_t c = 0; c < args.ColumnCount(); c++) {
		args.data[c].Flatten(count);
	}
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		const auto catalog_name = ArgString(args, 0, i);
		const auto schema_name = ArgString(args, 1, i);
		const auto name = ArgString(args, 2, i);
		const auto open_udf = ArgString(args, 3, i);
		const auto next_udf = ArgString(args, 4, i);
		const auto close_udf = ArgString(args, 5, i);
		if (catalog_name.empty() || schema_name.empty() || name.empty()) {
			throw InvalidInputException("vcat_create_stream_function: catalog, schema and name must all be non-empty");
		}
		if (open_udf.empty() || next_udf.empty() || close_udf.empty()) {
			throw InvalidInputException("vcat_create_stream_function: open, next and close UDF names are all required");
		}

		auto stream_info = make_shared_ptr<StreamFunctionInfo>(name, open_udf, next_udf, close_udf);
		TableFunction tf(name, {}, ArrowTableFunction::ArrowScanFunction, OpenStreamAndBindSchema,
		                 ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
		tf.varargs = LogicalType::ANY;
		tf.function_info = std::move(stream_info);

		CreateTableFunctionInfo create_info(std::move(tf));
		create_info.catalog = catalog_name;
		create_info.schema = schema_name;
		create_info.name = name;
		// Its constructor hardcodes internal=true, which is right for a built-in registered into the
		// system catalog and rejected outright anywhere else. This one is a user-visible entry in a
		// user catalog.
		create_info.internal = false;
		// Re-registering the same name replaces it: a host that reconnects re-registers, and
		// refusing there would make the second connection fail on a name the first still owns.
		create_info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;

		auto &catalog = Catalog::GetCatalog(context, catalog_name);
		// A SELECT is not DDL as far as the transaction is concerned, so the catalog set refuses the
		// insert ("this database is not marked as modified") unless we declare it here — the same
		// declaration the planner makes for a CREATE statement.
		MetaTransaction::Get(context).ModifyDatabase(catalog.GetAttached(),
		                                             DatabaseModificationType::CREATE_CATALOG_ENTRY);
		catalog.CreateTableFunction(context, create_info);
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

// vcat_drop_stream_function(catalog, schema, name) -> 'ok'
void DropStreamFunctionEntries(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	const auto count = args.size();
	for (idx_t c = 0; c < args.ColumnCount(); c++) {
		args.data[c].Flatten(count);
	}
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		DropInfo drop;
		drop.type = CatalogType::TABLE_FUNCTION_ENTRY;
		drop.catalog = ArgString(args, 0, i);
		drop.schema = ArgString(args, 1, i);
		drop.name = ArgString(args, 2, i);
		// Teardown runs on paths that may already have dropped it; absence is not an error.
		drop.if_not_found = OnEntryNotFound::RETURN_NULL;

		auto &catalog = Catalog::GetCatalog(context, drop.catalog);
		MetaTransaction::Get(context).ModifyDatabase(catalog.GetAttached(),
		                                             DatabaseModificationType::DROP_CATALOG_ENTRY);
		catalog.DropEntry(context, drop);
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

struct StreamFunctionRow {
	std::string catalog;
	std::string schema;
	std::string entry_name;
	std::string function_name;
	std::string open_udf;
	std::string next_udf;
	std::string close_udf;
};

struct StreamFunctionsBindData : public TableFunctionData {
	vector<StreamFunctionRow> rows;
};

struct StreamFunctionsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

// An entry is one of ours when its bind is OpenStreamAndBindSchema; the pointer comparison is
// in-binary now that virtual_catalog is the only extension creating these.
void CollectStreamFunctions(ClientContext &context, Catalog &catalog, vector<StreamFunctionRow> &out) {
	vector<reference<SchemaCatalogEntry>> schemas;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) { schemas.push_back(s); });
	for (auto &sref : schemas) {
		auto &schema = sref.get();
		schema.Scan(context, CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry &e) {
			// Table macros share this catalog set, so the scan yields them too.
			if (e.type != CatalogType::TABLE_FUNCTION_ENTRY) {
				return;
			}
			auto &fn_entry = e.Cast<TableFunctionCatalogEntry>();
			for (auto &fn : fn_entry.functions.functions) {
				if (fn.bind != OpenStreamAndBindSchema || !fn.function_info) {
					continue;
				}
				auto &info = fn.function_info->Cast<StreamFunctionInfo>();
				out.push_back({catalog.GetName(), schema.name, e.name, info.function_name, info.open_udf, info.next_udf,
				               info.close_udf});
			}
		});
	}
}

unique_ptr<FunctionData> StreamFunctionsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	names = {"catalog", "schema", "entry_name", "function_name", "open_udf", "next_udf", "close_udf"};
	return_types.assign(names.size(), LogicalType::VARCHAR);

	auto result = make_uniq<StreamFunctionsBindData>();
	for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
		CollectStreamFunctions(context, db->GetCatalog(), result->rows);
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> StreamFunctionsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<StreamFunctionsState>();
}

void StreamFunctionsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<StreamFunctionsBindData>();
	auto &state = data_p.global_state->Cast<StreamFunctionsState>();
	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog));
		output.SetValue(1, count, Value(row.schema));
		output.SetValue(2, count, Value(row.entry_name));
		output.SetValue(3, count, Value(row.function_name));
		output.SetValue(4, count, Value(row.open_udf));
		output.SetValue(5, count, Value(row.next_udf));
		output.SetValue(6, count, Value(row.close_udf));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterStreamFunctionCreators(ExtensionLoader &loader) {
	ScalarFunction create_func("vcat_create_stream_function",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                            LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           LogicalType::VARCHAR, CreateStreamFunctionEntries);
	loader.RegisterFunction(create_func);

	ScalarFunction drop_func("vcat_drop_stream_function",
	                         {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                         DropStreamFunctionEntries);
	loader.RegisterFunction(drop_func);

	TableFunction list_func("vcat_stream_functions", {}, StreamFunctionsScan, StreamFunctionsBind,
	                        StreamFunctionsInitGlobal);
	loader.RegisterFunction(std::move(list_func));
}

} // namespace vcat
} // namespace duckdb
