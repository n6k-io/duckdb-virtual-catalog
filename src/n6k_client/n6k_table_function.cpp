#include "n6k_table_function.hpp"
#include "n6k_rpc_function.hpp"
#include "n6k_version_function.hpp"
#include "n6k_agg_pushdown.hpp"
#include "n6k_capabilities_function.hpp"
#include "n6k_thread_selftest.hpp"
#include "n6k_io_thread_selftest.hpp"
#include "ws_send_after_start_selftest.hpp"
#include "hello_ns_selftest.hpp"
#include "ws_fd_hub_selftest.hpp"
#include "n6k_async_scan.hpp"
#include "n6k_parse_sql_get_tables.hpp"
#include "n6k_split_statements.hpp"
#include "n6k_table_permissions.hpp"
#include "n6k_storage.hpp"
#include "n6k_secret.hpp"
#include "n6k_login_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void LoadInternal(ExtensionLoader &loader) {
	RegisterN6kRpc(loader);
	RegisterN6kVersion(loader);
	RegisterN6kSelftestThreadParkWake(loader);
	RegisterN6kCapabilities(loader);
	RegisterN6kSelftestIoThreadDoorbell(loader);
	RegisterN6kSelftestSendAfterStart(loader);
	RegisterN6kSelftestHelloNsDistinct(loader);
	RegisterN6kSelftestFdHubRouting(loader);
	// Before the async-scan rule: OptimizerExtensions run in registration order, so this sees a
	// pristine LogicalGet. (The rewritten node has no children, so the async rule then skips it.)
	RegisterN6kAggregatePushdown(loader);
	RegisterN6kAsyncScan(loader);
	RegisterN6kParseSqlGetTables(loader);
	RegisterN6kSplitStatements(loader);
	RegisterN6kSecret(loader);
	RegisterN6kLogin(loader);
	RegisterN6kTablePermissions(loader);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "n6k", CreateN6kStorageExtension());
}

} // namespace duckdb
