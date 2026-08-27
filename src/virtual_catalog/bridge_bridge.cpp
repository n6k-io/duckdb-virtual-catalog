#include "bridge_bridge.hpp"
#include "shared_registry.hpp"
#include "sql_escape.hpp"
#include "bridge_catalog.hpp"
#include "vcat_catalog.hpp"
#include "vcat_schema_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

// Per-.so-load nonce — used to detect when source and target loaded different copies of the extension.
static const string &LoadNonce() {
	static string nonce = UUID::ToString(UUID::GenerateRandomUUID());
	return nonce;
}

static vcat::SharedRegistry<BridgeInfo> &BridgeRegistry() {
	static vcat::SharedRegistry<BridgeInfo> instance;
	return instance;
}

static mutex &PendingMutex() {
	static mutex instance;
	return instance;
}

static unordered_map<string, PendingSource> &PendingSources() {
	static unordered_map<string, PendingSource> instance;
	return instance;
}

static constexpr int64_t PENDING_SOURCE_TTL_SECONDS = 30;

// Caller must hold PendingMutex()
static void PurgeExpiredPendingSources() {
	auto now = steady_clock::now();
	auto &pending = PendingSources();
	for (auto it = pending.begin(); it != pending.end();) {
		auto elapsed = duration_cast<std::chrono::seconds>(now - it->second.created_at).count();
		if (elapsed > PENDING_SOURCE_TTL_SECONDS) {
			it = pending.erase(it);
		} else {
			++it;
		}
	}
}

shared_ptr<BridgeInfo> GetBridge(const string &bridge_id) {
	return BridgeRegistry().Get(bridge_id);
}

bool RemoveBridge(const string &bridge_id) {
	return BridgeRegistry().Erase(bridge_id);
}

static case_insensitive_map_t<string> MapRowToCaseInsensitiveEntries(Vector &map_vec, idx_t row) {
	auto list_data = ListVector::GetData(map_vec);
	auto &key_vec = MapVector::GetKeys(map_vec);
	auto &val_vec = MapVector::GetValues(map_vec);
	auto key_data = FlatVector::GetData<string_t>(key_vec);
	auto val_data = FlatVector::GetData<string_t>(val_vec);

	case_insensitive_map_t<string> result;
	auto list = list_data[row];
	for (idx_t entry_idx = 0; entry_idx < list.length; entry_idx++) {
		auto idx = list.offset + entry_idx;
		auto key = key_data[idx].GetString();
		if (!key.empty()) {
			result[key] = val_data[idx].GetString();
		}
	}
	return result;
}

static vector<string> DiscoverPrimaryKeyColumns(Connection &conn, const string &catalog, const string &schema,
                                                const string &table) {
	auto esc_catalog = vcat::EscapeSqlLiteral(catalog);
	auto esc_schema = vcat::EscapeSqlLiteral(schema);
	auto esc_table = vcat::EscapeSqlLiteral(table);

	// Method 1: duckdb_constraints (works for postgres, native duckdb)
	auto result = conn.SendQuery("SELECT constraint_column_names FROM duckdb_constraints() "
	                             "WHERE database_name = '" +
	                             esc_catalog + "' AND schema_name = '" + esc_schema + "' AND table_name = '" +
	                             esc_table + "' AND constraint_type = 'PRIMARY KEY'");
	if (!result->HasError()) {
		auto chunk = result->Fetch();
		if (chunk && chunk->size() > 0) {
			auto val = chunk->GetValue(0, 0);
			if (!val.IsNull() && val.type().id() == LogicalTypeId::LIST) {
				auto &children = ListValue::GetChildren(val);
				vector<string> pk_cols;
				for (auto &child : children) {
					pk_cols.push_back(child.ToString());
				}
				if (!pk_cols.empty()) {
					return pk_cols;
				}
			}
		}
	}

	// Method 2: query remote information_schema (works for mysql)
	auto qualified_is = KeywordHelper::WriteOptionallyQuoted(catalog) + ".information_schema.key_column_usage";
	result = conn.SendQuery("SELECT column_name FROM " + qualified_is + " WHERE table_schema = '" + esc_schema +
	                        "' AND table_name = '" + esc_table +
	                        "' AND constraint_name = 'PRIMARY' ORDER BY ordinal_position");
	if (!result->HasError()) {
		vector<string> pk_cols;
		while (true) {
			auto chunk = result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t row = 0; row < chunk->size(); row++) {
				pk_cols.push_back(chunk->GetValue(0, row).ToString());
			}
		}
		if (!pk_cols.empty()) {
			return pk_cols;
		}
	}

	return {};
}

// Must be called on the SOURCE connection; captures its DatabaseInstance and returns a nonce token.
static void ValidateSourceAndIssueBridgeToken(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	auto &bridge_id_vec = args.data[0];
	auto &source_catalog_vec = args.data[1];
	auto &source_schema_vec = args.data[2];
	auto &permissions_vec = args.data[3];
	auto &pk_overrides_vec = args.data[4];

	bridge_id_vec.Flatten(count);
	source_catalog_vec.Flatten(count);
	source_schema_vec.Flatten(count);
	permissions_vec.Flatten(count);
	pk_overrides_vec.Flatten(count);

	auto bridge_ids = FlatVector::GetData<string_t>(bridge_id_vec);
	auto source_catalogs = FlatVector::GetData<string_t>(source_catalog_vec);
	auto source_schemas = FlatVector::GetData<string_t>(source_schema_vec);

	auto &perm_key_vec = MapVector::GetKeys(permissions_vec);
	auto &perm_val_vec = MapVector::GetValues(permissions_vec);
	perm_key_vec.Flatten(ListVector::GetListSize(permissions_vec));
	perm_val_vec.Flatten(ListVector::GetListSize(permissions_vec));

	auto &pk_key_vec = MapVector::GetKeys(pk_overrides_vec);
	auto &pk_val_vec = MapVector::GetValues(pk_overrides_vec);
	pk_key_vec.Flatten(ListVector::GetListSize(pk_overrides_vec));
	pk_val_vec.Flatten(ListVector::GetListSize(pk_overrides_vec));

	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		auto source_catalog = source_catalogs[i].GetString();
		auto source_schema = source_schemas[i].GetString();

		auto raw_perms = MapRowToCaseInsensitiveEntries(permissions_vec, i);
		case_insensitive_map_t<TablePermission> permissions;
		for (auto &kv : raw_perms) {
			if (kv.first.find('.') != string::npos) {
				throw IOException("virtual_catalog: permission key '%s' must be a bare table name (no dots)", kv.first);
			}
			auto lower_perm = StringUtil::Lower(kv.second);
			if (lower_perm == "read") {
				permissions[kv.first] = TablePermission::READ;
			} else if (lower_perm == "readwrite") {
				permissions[kv.first] = TablePermission::READWRITE;
			} else {
				throw IOException("virtual_catalog: invalid permission '%s' for table '%s'. "
				                  "Valid values: 'read', 'readwrite'",
				                  kv.second, kv.first);
			}
		}

		if (permissions.empty()) {
			throw IOException("virtual_catalog: permissions cannot be empty");
		}

		auto raw_pk_overrides = MapRowToCaseInsensitiveEntries(pk_overrides_vec, i);
		case_insensitive_map_t<vector<string>> pk_overrides;
		for (auto &kv : raw_pk_overrides) {
			auto cols = StringUtil::Split(kv.second, ',');
			vector<string> trimmed;
			for (auto &col : cols) {
				StringUtil::Trim(col);
				if (!col.empty()) {
					trimmed.push_back(col);
				}
			}
			if (!trimmed.empty()) {
				pk_overrides[kv.first] = std::move(trimmed);
			}
		}

		auto source_conn = make_uniq<Connection>(*context.db);
		for (auto &kv : permissions) {
			auto qualified = KeywordHelper::WriteOptionallyQuoted(source_catalog) + "." +
			                 KeywordHelper::WriteOptionallyQuoted(source_schema) + "." +
			                 KeywordHelper::WriteOptionallyQuoted(kv.first);
			auto check_result = source_conn->SendQuery("SELECT 1 FROM " + qualified + " LIMIT 0");
			if (check_result->HasError()) {
				throw IOException("virtual_catalog: table '%s' not found in %s.%s on source", kv.first, source_catalog,
				                  source_schema);
			}
		}

		case_insensitive_map_t<vector<string>> primary_keys;
		for (auto &kv : permissions) {
			if (kv.second != TablePermission::READWRITE) {
				continue;
			}
			auto pk_it = pk_overrides.find(kv.first);
			if (pk_it != pk_overrides.end()) {
				primary_keys[kv.first] = pk_it->second;
				continue;
			}
			auto pk_cols = DiscoverPrimaryKeyColumns(*source_conn, source_catalog, source_schema, kv.first);
			if (pk_cols.empty()) {
				throw IOException("virtual_catalog: writable table '%s' has no discoverable primary key; "
				                  "provide a primary_keys override",
				                  kv.first);
			}
			primary_keys[kv.first] = std::move(pk_cols);
		}

		auto uuid = UUID::GenerateRandomUUID();
		auto token = LoadNonce() + ":" + UUID::ToString(uuid);

		PendingSource pending;
		pending.source_db = context.db;
		pending.token = token;
		pending.created_at = steady_clock::now();
		pending.source_catalog = source_catalog;
		pending.source_schema = source_schema;
		pending.permissions = std::move(permissions);
		pending.primary_keys = std::move(primary_keys);

		{
			lock_guard<mutex> lock(PendingMutex());
			PurgeExpiredPendingSources();
			if (PendingSources().find(bridge_id) != PendingSources().end()) {
				throw IOException("virtual_catalog: a source is already registered for bridge '%s'", bridge_id);
			}
			PendingSources()[bridge_id] = std::move(pending);
		}

		result_data[i] = StringVector::AddString(result, token);
	}
}

// Must be called on the TARGET connection; injects bridge schema into an existing catalog.
static void AttachPendingSourceToSchema(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();

	auto &bridge_id_vec = args.data[0];
	auto &token_vec = args.data[1];
	auto &name_vec = args.data[2];
	auto &target_schema_vec = args.data[3];

	bridge_id_vec.Flatten(count);
	token_vec.Flatten(count);
	name_vec.Flatten(count);
	target_schema_vec.Flatten(count);

	auto bridge_ids = FlatVector::GetData<string_t>(bridge_id_vec);
	auto tokens = FlatVector::GetData<string_t>(token_vec);
	auto names = FlatVector::GetData<string_t>(name_vec);
	auto target_schemas = FlatVector::GetData<string_t>(target_schema_vec);

	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		auto token = tokens[i].GetString();
		auto target_catalog_name = names[i].GetString();
		auto target_schema = target_schemas[i].GetString();

		auto colon_pos = token.find(':');
		if (colon_pos == string::npos) {
			throw PermissionException("virtual_catalog: invalid token format for bridge '%s'", bridge_id);
		}
		auto token_nonce = token.substr(0, colon_pos);
		if (token_nonce != LoadNonce()) {
			throw PermissionException(
			    "virtual_catalog: source and target loaded different copies of the virtual_catalog extension. "
			    "Ensure both connections use the same extension installation.");
		}

		// Checked before the pending source is consumed, not after: taking the name is destructive
		// (the token, the captured source DatabaseInstance and the discovered primary keys all go
		// with it), so failing afterwards would force the caller back to vcat_register_source on the
		// source connection to retry. The claim below is what actually reserves the name.
		if (BridgeRegistry().Contains(bridge_id)) {
			throw IOException("virtual_catalog: bridge '%s' already exists", bridge_id);
		}

		shared_ptr<DatabaseInstance> source_db;
		string source_catalog;
		string source_schema;
		case_insensitive_map_t<TablePermission> permissions;
		case_insensitive_map_t<vector<string>> primary_keys;
		{
			lock_guard<mutex> lock(PendingMutex());
			PurgeExpiredPendingSources();
			auto it = PendingSources().find(bridge_id);
			if (it == PendingSources().end()) {
				throw PermissionException("virtual_catalog: no registered source for bridge '%s'. "
				                          "Call vcat_register_source on the source connection first.",
				                          bridge_id);
			}
			if (it->second.token != token) {
				throw PermissionException("virtual_catalog: invalid setup token for bridge '%s'", bridge_id);
			}
			source_db = it->second.source_db;
			source_catalog = std::move(it->second.source_catalog);
			source_schema = std::move(it->second.source_schema);
			permissions = std::move(it->second.permissions);
			primary_keys = std::move(it->second.primary_keys);
			PendingSources().erase(it);
		}

		auto target_catalog_entry = Catalog::GetCatalogEntry(context, target_catalog_name);
		if (!target_catalog_entry) {
			throw CatalogException("virtual_catalog: target catalog '%s' does not exist", target_catalog_name);
		}
		auto *vcat = dynamic_cast<VirtualCatalog *>(target_catalog_entry.get());
		if (!vcat) {
			throw CatalogException("virtual_catalog: target catalog '%s' is not TYPE virtual_catalog; ATTACH with "
			                       "(TYPE virtual_catalog) first",
			                       target_catalog_name);
		}

		EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, target_schema);
		auto &db_instance = target_catalog_entry->GetAttached().GetDatabase();
		auto sys_transaction = CatalogTransaction::GetSystemTransaction(db_instance);
		auto schema_entry = vcat->LookupSchema(sys_transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
		if (!schema_entry) {
			throw CatalogException("virtual_catalog: schema '%s.%s' does not exist; CREATE SCHEMA first",
			                       target_catalog_name, target_schema);
		}
		auto *local_schema = dynamic_cast<VirtualCatalogSchemaEntry *>(schema_entry.get());
		if (!local_schema) {
			throw CatalogException("virtual_catalog: schema '%s.%s' is not bridge-capable", target_catalog_name,
			                       target_schema);
		}

		auto bridge_info = make_shared_ptr<BridgeInfo>();
		bridge_info->bridge_id = bridge_id;
		bridge_info->source_db = source_db;
		bridge_info->source_catalog = std::move(source_catalog);
		bridge_info->source_schema = std::move(source_schema);
		bridge_info->permissions = std::move(permissions);
		bridge_info->primary_keys = std::move(primary_keys);

		// Atomic claim: two connections racing to set up the same bridge_id cannot both win, which
		// the Contains() pre-check above cannot guarantee on its own.
		if (!BridgeRegistry().Insert(bridge_id, bridge_info)) {
			throw IOException("virtual_catalog: bridge '%s' already exists", bridge_id);
		}

		auto phantom = make_shared_ptr<BridgeCatalog>(target_catalog_entry->GetAttached(), bridge_info);

		local_schema->SetBridge(bridge_info, phantom);

		result_data[i] = StringVector::AddString(result, "ok");
	}
}

// The teardown half of vcat_setup_bridge, and the only thing that releases the source
// DatabaseInstance the registry pinned. Mirrors vcat_unregister_provider: same (catalog, schema)
// arguments, same order — unbind from the schema first so racing plan compilation stops routing
// here, then drop the registry entry.
static void DetachBridgeFromSchemaAndUnregister(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);
	args.data[1].Flatten(count);

	auto catalogs = FlatVector::GetData<string_t>(args.data[0]);
	auto schemas = FlatVector::GetData<string_t>(args.data[1]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto target_catalog_name = catalogs[i].GetString();
		auto target_schema_name = schemas[i].GetString();

		auto catalog_entry = Catalog::GetCatalogEntry(context, target_catalog_name);
		if (!catalog_entry) {
			throw CatalogException("virtual_catalog: target catalog '%s' does not exist", target_catalog_name);
		}
		auto *vcat = dynamic_cast<VirtualCatalog *>(catalog_entry.get());
		if (!vcat) {
			throw CatalogException(
			    "virtual_catalog: catalog '%s' is not TYPE virtual_catalog; ATTACH with (TYPE virtual_catalog) first",
			    target_catalog_name);
		}

		EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, target_schema_name);
		auto &db_instance = catalog_entry->GetAttached().GetDatabase();
		auto sys_transaction = CatalogTransaction::GetSystemTransaction(db_instance);
		auto schema_entry = vcat->LookupSchema(sys_transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
		auto *local_schema = schema_entry ? dynamic_cast<VirtualCatalogSchemaEntry *>(schema_entry.get()) : nullptr;
		if (!local_schema) {
			throw CatalogException("virtual_catalog: no bridge registered for %s.%s", target_catalog_name,
			                       target_schema_name);
		}

		auto bridge_id = local_schema->CurrentBridgeId();
		if (bridge_id.empty()) {
			throw CatalogException("virtual_catalog: no bridge registered for %s.%s", target_catalog_name,
			                       target_schema_name);
		}
		local_schema->SetBridge(nullptr, nullptr);
		RemoveBridge(bridge_id);
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void RegisterBridgeFunctions(ExtensionLoader &loader) {
	ScalarFunction register_source("vcat_register_source",
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR),
	                                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	                               LogicalType::VARCHAR, ValidateSourceAndIssueBridgeToken, nullptr, nullptr, nullptr,
	                               nullptr, LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
	loader.RegisterFunction(register_source);

	ScalarFunction setup_bridge(
	    "vcat_setup_bridge", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, AttachPendingSourceToSchema, nullptr, nullptr, nullptr, nullptr,
	    LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
	loader.RegisterFunction(setup_bridge);

	ScalarFunction unregister_bridge("vcat_unregister_bridge", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::VARCHAR, DetachBridgeFromSchemaAndUnregister, nullptr, nullptr,
	                                 nullptr, nullptr, LogicalType(LogicalTypeId::INVALID),
	                                 FunctionStability::VOLATILE);
	loader.RegisterFunction(unregister_bridge);
}

} // namespace duckdb
