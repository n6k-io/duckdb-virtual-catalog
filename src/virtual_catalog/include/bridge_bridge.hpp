#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

enum class TablePermission : uint8_t { READ = 0, READWRITE = 1 };

struct BridgeInfo {
	string bridge_id;
	shared_ptr<DatabaseInstance> source_db;
	string source_catalog;
	string source_schema;
	case_insensitive_map_t<TablePermission> permissions;
	case_insensitive_map_t<vector<string>> primary_keys;
	mutex mtx;

	bool HasReadPermission(const string &name) {
		auto it = permissions.find(name);
		if (it == permissions.end()) {
			return false;
		}
		return it->second == TablePermission::READ || it->second == TablePermission::READWRITE;
	}

	bool HasWritePermission(const string &name) {
		auto it = permissions.find(name);
		if (it == permissions.end()) {
			return false;
		}
		return it->second == TablePermission::READWRITE;
	}

	bool HasAnyPermission(const string &name) {
		return permissions.find(name) != permissions.end();
	}

	string QualifiedSourceTable(const string &bare_table) const {
		return KeywordHelper::WriteOptionallyQuoted(source_catalog) + "." +
		       KeywordHelper::WriteOptionallyQuoted(source_schema) + "." +
		       KeywordHelper::WriteOptionallyQuoted(bare_table);
	}
};

struct PendingSource {
	shared_ptr<DatabaseInstance> source_db;
	string token;
	steady_clock::time_point created_at;
	string source_catalog;
	string source_schema;
	case_insensitive_map_t<TablePermission> permissions;
	case_insensitive_map_t<vector<string>> primary_keys;
};

shared_ptr<BridgeInfo> GetBridge(const string &bridge_id);

//! Drop the registry's reference. False if there was no such bridge. Detach the bridge from its
//! schema (VirtualCatalogSchemaEntry::SetBridge(nullptr, nullptr)) BEFORE calling this, so a racing
//! plan compilation stops routing here first — the provider path is ordered the same way.
//!
//! This is what releases BridgeInfo::source_db. Until it runs, the registry holds a shared_ptr to
//! the source DatabaseInstance and DETACH on either side frees nothing.
bool RemoveBridge(const string &bridge_id);

void RegisterBridgeFunctions(ExtensionLoader &loader);

} // namespace duckdb
