#include "serve_bind_common.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {
namespace n6k {

// Zero-arg serving covers everything the caller has open. System/temp catalogs are excluded:
// they are not attached data and a client has no use for them.
//
// The startup in-memory database is excluded too, when anything else is attached. A connection
// opened with no path carries one it never asked for — `duckdb.connect()` names it `memory` — and
// serving it alongside a deliberately attached catalog is not cosmetic: two catalogs make the
// session multiplexed, which suppresses the unprompted HELLO_ACK and requires the client to name a
// catalog in its handshake. A host that attached exactly one catalog would get a changed wire.
//
// Only the *in-memory* startup database is dropped. `duckdb mydata.duckdb` asked for `mydata`
// explicitly, so it stays even once something else is attached; and a connection whose only catalog
// is the in-memory default still serves it, since that is plainly what was meant.
static vector<std::string> AttachedUserCatalogs(ClientContext &context) {
	vector<std::string> names;
	vector<std::string> requested;
	for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
		if (db->IsSystem() || db->IsTemporary()) {
			continue;
		}
		names.push_back(db->GetName());
		if (!db->IsInitialDatabase() || !db->GetCatalog().InMemory()) {
			requested.push_back(db->GetName());
		}
	}
	return requested.empty() ? names : requested;
}

vector<std::string> ResolveServedCatalogs(ClientContext &context, const vector<Value> &inputs, idx_t first_catalog,
                                          const char *fn_name) {
	if (inputs.size() <= first_catalog) {
		// The set is frozen here: a later ATTACH is not picked up by a running serve.
		auto catalogs = AttachedUserCatalogs(context);
		if (catalogs.empty()) {
			throw BinderException("%s: no attached catalogs to serve; ATTACH one or name it explicitly", fn_name);
		}
		return catalogs;
	}

	vector<std::string> catalogs;
	case_insensitive_set_t seen;
	for (idx_t i = first_catalog; i < inputs.size(); i++) {
		auto &arg = inputs[i];
		if (arg.IsNull()) {
			throw BinderException("%s requires a catalog argument: CALL %s('mydata')", fn_name, fn_name);
		}
		auto catalog_name = arg.GetValue<std::string>();
		try {
			(void)Catalog::GetCatalog(context, catalog_name);
		} catch (const std::exception &) {
			throw BinderException("%s: catalog \"%s\" is not attached; ATTACH it before serving", fn_name,
			                      catalog_name);
		}
		// Two sessions must never resolve to the same catalog by accident.
		if (!seen.insert(catalog_name).second) {
			throw BinderException("%s: catalog \"%s\" listed more than once", fn_name, catalog_name);
		}
		catalogs.push_back(std::move(catalog_name));
	}
	return catalogs;
}

} // namespace n6k
} // namespace duckdb
