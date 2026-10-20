#include "lockdown.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

namespace {

void SetGlobal(DatabaseInstance &db, const char *name, const Value &value) {
	auto &config = DBConfig::GetConfig(db);
	auto option = DBConfig::GetOptionByName(name);
	if (!option) {
		throw InternalException("virtual_catalog_lockdown: unknown setting \"%s\"", name);
	}
	config.SetOption(&db, *option, value);
}

//! DuckDB reads '-<anything>', 'none' and 'null' as no limit at all.
void ThrowUnlessFiniteMemoryLimit(const string &text) {
	if (text.empty()) {
		throw InvalidInputException("virtual_catalog_lockdown: memory_limit must not be empty");
	}
	idx_t bytes;
	try {
		bytes = DBConfig::ParseMemoryLimit(text);
	} catch (const std::exception &ex) {
		throw InvalidInputException("virtual_catalog_lockdown: memory_limit '%s' is not a size: %s", text,
		                            ErrorData(ex).RawMessage());
	}
	if (bytes == NumericLimits<idx_t>::Maximum()) {
		throw InvalidInputException("virtual_catalog_lockdown: memory_limit '%s' must be a finite size, e.g. '512MB'",
		                            text);
	}
}

//! Every function-like set of a DuckSchemaEntry; macros and aggregates share the scalar set.
const CatalogType FUNCTION_SETS[] = {CatalogType::SCALAR_FUNCTION_ENTRY, CatalogType::TABLE_FUNCTION_ENTRY,
                                     CatalogType::PRAGMA_FUNCTION_ENTRY};

vector<reference<CatalogEntry>> ResolveSystemFunctions(ClientContext &context, const Value &names) {
	if (names.IsNull()) {
		throw InvalidInputException("virtual_catalog_lockdown: drop_functions must not be NULL");
	}
	auto &catalog = Catalog::GetSystemCatalog(context);
	vector<reference<CatalogEntry>> entries;
	case_insensitive_set_t seen;
	for (auto &name : ListValue::GetChildren(names)) {
		if (name.IsNull()) {
			throw InvalidInputException("virtual_catalog_lockdown: drop_functions must not contain NULL");
		}
		auto text = StringValue::Get(name);
		if (!seen.insert(text).second) {
			continue;
		}
		bool found = false;
		for (auto type : FUNCTION_SETS) {
			auto entry = catalog.GetEntry(context, type, DEFAULT_SCHEMA, text, OnEntryNotFound::RETURN_NULL);
			if (entry) {
				entries.push_back(*entry);
				found = true;
			}
		}
		if (!found) {
			throw InvalidInputException("virtual_catalog_lockdown: no system function named \"%s\"", text);
		}
	}
	return entries;
}

//! On its own connection so the drops commit before the settings, which cannot roll back, are applied:
//! in the caller's transaction a ROLLBACK or an unfetched result would undo them and leave the lock.
void DropSystemFunctions(DatabaseInstance &db, const Value &names) {
	Connection con(db);
	auto &context = *con.context;
	context.RunFunctionInTransaction([&]() {
		auto entries = ResolveSystemFunctions(context, names);
		auto &catalog = Catalog::GetSystemCatalog(context);
		MetaTransaction::Get(context).ModifyDatabase(catalog.GetAttached(),
		                                             DatabaseModificationType::DROP_CATALOG_ENTRY);
		for (auto &entry : entries) {
			DropInfo drop;
			drop.type = entry.get().type;
			drop.schema = DEFAULT_SCHEMA;
			drop.name = entry.get().name;
			drop.allow_drop_internal = true;
			catalog.DropEntry(context, drop);
		}
	});
}

void LockdownFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &db = DatabaseInstance::GetDatabase(state.GetContext());
	auto count = args.size();
	if (count == 0) {
		return;
	}
	if (!state.GetContext().transaction.IsAutoCommit()) {
		throw PermissionException("virtual_catalog_lockdown: cannot run inside a transaction");
	}
	if (count > 1) {
		throw InvalidInputException("virtual_catalog_lockdown: call it once, not once per row");
	}
	auto memory_limit = args.data[0].GetValue(0);
	auto threads = args.data[1].GetValue(0);
	if (memory_limit.IsNull() || threads.IsNull()) {
		throw InvalidInputException("virtual_catalog_lockdown: memory_limit and threads must not be NULL");
	}
	if (Settings::Get<LockConfigurationSetting>(DBConfig::GetConfig(db))) {
		throw PermissionException("virtual_catalog_lockdown: the configuration is already locked");
	}
	ThrowUnlessFiniteMemoryLimit(memory_limit.ToString());
	if (args.ColumnCount() > 2) {
		DropSystemFunctions(db, args.data[2].GetValue(0));
	}

	// temp_directory refuses changes once external access is off, so it goes first.
	SetGlobal(db, "temp_directory", Value(""));
	SetGlobal(db, "memory_limit", memory_limit);
	SetGlobal(db, "threads", threads);
	SetGlobal(db, "autoload_known_extensions", Value::BOOLEAN(false));
	SetGlobal(db, "autoinstall_known_extensions", Value::BOOLEAN(false));
	SetGlobal(db, "allow_community_extensions", Value::BOOLEAN(false));
	SetGlobal(db, "allow_persistent_secrets", Value::BOOLEAN(false));
	SetGlobal(db, "disabled_filesystems", Value("LocalFileSystem"));
	SetGlobal(db, "enable_external_access", Value::BOOLEAN(false));
	SetGlobal(db, "lock_configuration", Value::BOOLEAN(true));

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ok");
}

} // namespace

void RegisterLockdownFunction(ExtensionLoader &loader) {
	ScalarFunctionSet set("virtual_catalog_lockdown");
	for (auto &arguments :
	     {vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT},
	      vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::LIST(LogicalType::VARCHAR)}}) {
		ScalarFunction lockdown(arguments, LogicalType::VARCHAR, LockdownFunc);
		lockdown.stability = FunctionStability::VOLATILE;
		lockdown.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
		set.AddFunction(lockdown);
	}
	loader.RegisterFunction(set);
}

} // namespace duckdb
