#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "crossing.hpp"

namespace duckdb {

static constexpr const char *CROSSING_TRANSACTIONS_KEY = "vcat_v2_source_transactions";

class CrossingTransactions : public ClientContextState {
public:
	CrossingTransaction &TransactionFor(ClientContext &context, CrossingSource &source);

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		Finish(true);
	}
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		Finish(false);
	}

	static shared_ptr<CrossingTransactions> Get(ClientContext &context) {
		return context.registered_state->GetOrCreate<CrossingTransactions>(CROSSING_TRANSACTIONS_KEY);
	}

private:
	void Finish(bool commit);

	mutex lock;
	vector<pair<CrossingSource *, unique_ptr<CrossingTransaction>>> begun;
};

CrossingTransaction &TransactionFor(ClientContext &context, CrossingSource &source);

} // namespace duckdb
