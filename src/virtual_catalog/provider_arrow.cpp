#include "provider_arrow.hpp"

#include "arrow_ipc_encode.hpp"
#include "arrow_frame_decoder.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "nanoarrow.h"

#include <cerrno>
#include <deque>

namespace duckdb {
namespace vcat_provider {

const char *const PRIMARY_KEY_METADATA_KEY = "vcat.primary_keys";
// Providers written against the pre-split extension still emit this. Read it so their UPDATE/DELETE
// keep working; drop once they have moved.
const char *const LEGACY_PRIMARY_KEY_METADATA_KEY = "n6k.primary_keys";

namespace {

// Drives the shared decoder over one payload and reports its own error text, which names the
// nanoarrow failure rather than "something went wrong decoding".
void PushOrThrow(vcat::ArrowFrameDecoder &decoder, const string &ipc_bytes, const char *what) {
	if (!decoder.PushFrame(ipc_bytes)) {
		throw IOException("%s: could not decode the Arrow IPC the provider returned: %s", what, decoder.ErrorMessage());
	}
}

vector<string> ReadPrimaryKeys(const ArrowSchema &schema) {
	if (!schema.metadata) {
		return vector<string>();
	}
	ArrowStringView value;
	value.data = nullptr;
	value.size_bytes = 0;
	if (ArrowMetadataGetValue(schema.metadata, ArrowCharView(PRIMARY_KEY_METADATA_KEY), &value) != NANOARROW_OK ||
	    !value.data || value.size_bytes <= 0) {
		value.data = nullptr;
		value.size_bytes = 0;
		if (ArrowMetadataGetValue(schema.metadata, ArrowCharView(LEGACY_PRIMARY_KEY_METADATA_KEY), &value) !=
		    NANOARROW_OK) {
			return vector<string>();
		}
	}
	if (!value.data || value.size_bytes <= 0) {
		return vector<string>();
	}
	vector<string> keys;
	for (auto &key : StringUtil::Split(string(value.data, NumericCast<size_t>(value.size_bytes)), ',')) {
		auto trimmed = key;
		StringUtil::Trim(trimmed);
		if (!trimmed.empty()) {
			keys.push_back(trimmed);
		}
	}
	return keys;
}

// One in-flight scan: every batch already decoded, handed out one per get_next.
struct PreloadedStream {
	ArrowSchema schema {};
	bool schema_owned = false;
	std::deque<ArrowArray> batches;
	string last_error;

	~PreloadedStream() {
		for (auto &batch : batches) {
			if (batch.release) {
				batch.release(&batch);
			}
		}
		if (schema_owned && schema.release) {
			schema.release(&schema);
		}
	}
};

int PreloadedGetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
	auto *self = static_cast<PreloadedStream *>(stream->private_data);
	if (!self->schema_owned) {
		self->last_error = "vcat_provider: stream has no schema";
		return EINVAL;
	}
	return ArrowSchemaDeepCopy(&self->schema, out) == NANOARROW_OK ? 0 : EIO;
}

int PreloadedGetNext(ArrowArrayStream *stream, ArrowArray *out) {
	auto *self = static_cast<PreloadedStream *>(stream->private_data);
	// An empty queue is end of stream, which the C data interface spells as a released array.
	out->release = nullptr;
	if (self->batches.empty()) {
		return 0;
	}
	*out = self->batches.front();
	self->batches.pop_front();
	return 0;
}

const char *PreloadedGetLastError(ArrowArrayStream *stream) {
	auto *self = static_cast<PreloadedStream *>(stream->private_data);
	return self->last_error.empty() ? nullptr : self->last_error.c_str();
}

void PreloadedRelease(ArrowArrayStream *stream) {
	delete static_cast<PreloadedStream *>(stream->private_data);
	stream->private_data = nullptr;
	stream->get_schema = nullptr;
	stream->get_next = nullptr;
	stream->get_last_error = nullptr;
	stream->release = nullptr;
}

} // namespace

DecodedProviderSchema DecodeSchemaMessage(ClientContext &context, const string &ipc_bytes, const char *what) {
	if (ipc_bytes.empty()) {
		throw IOException("%s: the provider returned an empty Arrow schema", what);
	}
	vcat::ArrowFrameDecoder decoder;
	PushOrThrow(decoder, ipc_bytes, what);
	if (!decoder.HasSchema()) {
		throw IOException("%s: the provider's Arrow payload carried no schema message", what);
	}
	const auto *schema = decoder.GetSchema();

	DecodedProviderSchema result;
	result.primary_keys = ReadPrimaryKeys(*schema);
	for (int64_t i = 0; i < schema->n_children; i++) {
		auto &child = *schema->children[i];
		result.column_names.push_back(child.name ? string(child.name) : string());
		result.column_types.push_back(ArrowType::GetArrowLogicalType(context, child)->GetDuckType(true));
	}
	return result;
}

void MakeStreamFromIpc(const string &ipc_bytes, ArrowArrayStream *out, const char *what) {
	auto self = make_uniq<PreloadedStream>();
	vcat::ArrowFrameDecoder decoder;
	PushOrThrow(decoder, ipc_bytes, what);
	if (!decoder.HasSchema()) {
		throw IOException("%s: the provider's Arrow payload carried no schema message", what);
	}
	if (ArrowSchemaDeepCopy(decoder.GetSchema(), &self->schema) != NANOARROW_OK) {
		throw IOException("%s: ArrowSchemaDeepCopy failed", what);
	}
	self->schema_owned = true;

	// A table with no rows decodes to no batches at all, which is a legitimate empty result rather
	// than a failure — get_next simply reports end of stream immediately.
	ArrowArray batch;
	while (decoder.TryPopBatch(&batch)) {
		self->batches.push_back(batch);
	}

	out->get_schema = PreloadedGetSchema;
	out->get_next = PreloadedGetNext;
	out->get_last_error = PreloadedGetLastError;
	out->release = PreloadedRelease;
	out->private_data = self.release();
}

TrailingColumns DescribeTrailingColumns(ClientContext &context, ArrowArrayStream &stream, idx_t count,
                                        const char *what) {
	TrailingColumns result;
	if (count == 0) {
		return result;
	}
	ArrowSchema schema;
	if (stream.get_schema(&stream, &schema) != 0) {
		throw IOException("%s: could not read the Arrow schema of the scan result", what);
	}
	auto column_count = NumericCast<idx_t>(schema.n_children);
	if (column_count < count) {
		if (schema.release) {
			schema.release(&schema);
		}
		throw IOException("%s: the provider returned %llu columns, fewer than the %llu primary-key columns it must "
		                  "append",
		                  what, static_cast<uint64_t>(column_count), static_cast<uint64_t>(count));
	}
	for (idx_t i = column_count - count; i < column_count; i++) {
		auto &child = *schema.children[i];
		result.types.push_back(ArrowType::GetArrowLogicalType(context, child)->GetDuckType(true));
		result.formats.push_back(child.format ? string(child.format) : string());
	}
	if (schema.release) {
		schema.release(&schema);
	}
	return result;
}

RowChunkBuilder::RowChunkBuilder(vector<LogicalType> types) : types_(std::move(types)) {
}

void RowChunkBuilder::BeginRow() {
	if (!current_) {
		current_ = make_uniq<DataChunk>();
		current_->Initialize(Allocator::DefaultAllocator(), types_);
		row_ = 0;
	}
	column_ = 0;
}

void RowChunkBuilder::Append(const Value &value) {
	if (!current_) {
		throw InternalException("vcat_provider: RowChunkBuilder::Append before BeginRow");
	}
	if (column_ >= types_.size()) {
		throw InternalException("vcat_provider: more values appended than the row has columns");
	}
	// Cast rather than trust: a key value read back out of an Arrow buffer carries the type that
	// buffer had, which need not be the column's own (an int32 key column read as INTEGER, say).
	current_->SetValue(column_, row_, value.DefaultCastAs(types_[column_]));
	column_++;
}

void RowChunkBuilder::EndRow(const char *what) {
	if (column_ != types_.size()) {
		throw IOException("%s: row has %llu values but the table has %llu columns", what,
		                  static_cast<uint64_t>(column_), static_cast<uint64_t>(types_.size()));
	}
	row_++;
	current_->SetCardinality(row_);
	if (row_ >= STANDARD_VECTOR_SIZE) {
		Flush();
	}
}

void RowChunkBuilder::Flush() {
	if (current_ && row_ > 0) {
		current_->SetCardinality(row_);
		chunks_.push_back(std::move(current_));
	}
	current_ = nullptr;
	row_ = 0;
}

vector<unique_ptr<DataChunk>> RowChunkBuilder::Finish() {
	Flush();
	return std::move(chunks_);
}

string EncodeChunksAsIpc(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names,
                         const vector<unique_ptr<DataChunk>> &chunks) {
	vcat::ArrowIpcStreamEncoder encoder(context, types, names);
	string out = encoder.SchemaMessage();
	for (auto &chunk : chunks) {
		if (!chunk || chunk->size() == 0) {
			continue;
		}
		out += encoder.EncodeChunk(*chunk);
	}
	out += encoder.TakeEndOfStreamMessage();
	return out;
}

int64_t CallWriteUdf(Connection &conn, const string &udf_name, const string &table_name, const string &arrow_ipc,
                     const vector<Value> &extra, const char *what) {
	string placeholders = "?, ?";
	for (idx_t i = 0; i < extra.size(); i++) {
		placeholders += ", ?";
	}
	auto statement =
	    conn.Prepare("SELECT " + KeywordHelper::WriteOptionallyQuoted(udf_name) + "(" + placeholders + ")");
	if (statement->HasError()) {
		statement->GetErrorObject().Throw(string(what) + " function \"" + udf_name + "\" is unusable: ");
	}
	// vector<Value>, not the variadic Execute: that overload routes std::string through
	// Value::CreateValue<string>, which builds a BLOB, so the VARCHAR table name never binds.
	vector<Value> params;
	params.emplace_back(table_name);
	params.push_back(Value::BLOB(reinterpret_cast<const_data_ptr_t>(arrow_ipc.data()), arrow_ipc.size()));
	for (auto &value : extra) {
		params.push_back(value);
	}
	auto result = statement->Execute(params);
	if (result->HasError()) {
		result->GetErrorObject().Throw(string(what) + " failed: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return 0;
	}
	auto value = chunk->GetValue(0, 0);
	return value.IsNull() ? 0 : value.GetValue<int64_t>();
}

} // namespace vcat_provider
} // namespace duckdb
