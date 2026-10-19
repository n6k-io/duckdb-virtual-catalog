#include "lockdown.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/settings.hpp"

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

void LockdownFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &db = DatabaseInstance::GetDatabase(state.GetContext());
	auto count = args.size();
	if (count == 0) {
		return;
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
	ScalarFunction lockdown("virtual_catalog_lockdown", {LogicalType::VARCHAR, LogicalType::BIGINT},
	                        LogicalType::VARCHAR, LockdownFunc);
	lockdown.stability = FunctionStability::VOLATILE;
	lockdown.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(lockdown);
}

} // namespace duckdb
