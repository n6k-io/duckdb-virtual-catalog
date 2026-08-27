#include "bridge_transaction.hpp"

namespace duckdb {

BridgeTransaction::BridgeTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

BridgeTransaction::~BridgeTransaction() = default;

BridgeTransactionManager::BridgeTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
}

Transaction &BridgeTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<BridgeTransaction>(*this, context);
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData BridgeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return {};
}

void BridgeTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void BridgeTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace duckdb
