#pragma once

#include "duckdb.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class BridgeCatalog;

class BridgeTransaction : public Transaction {
public:
	BridgeTransaction(TransactionManager &manager, ClientContext &context);
	~BridgeTransaction() override;
};

class BridgeTransactionManager : public TransactionManager {
public:
	explicit BridgeTransactionManager(AttachedDatabase &db);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<BridgeTransaction>> transactions;
};

} // namespace duckdb
