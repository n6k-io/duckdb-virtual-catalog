#include "crossing_transactions.hpp"

#include "duckdb/common/error_data.hpp"

namespace duckdb {

CrossingTransaction &CrossingTransactions::TransactionFor(ClientContext &context, CrossingSource &source) {
	lock_guard<mutex> guard(lock);
	for (auto &entry : begun) {
		if (entry.first == &source) {
			return *entry.second;
		}
	}
	begun.emplace_back(&source, source.Begin(context));
	return *begun.back().second;
}

void CrossingTransactions::Finish(bool commit) {
	vector<pair<CrossingSource *, unique_ptr<CrossingTransaction>>> resolving;
	{
		lock_guard<mutex> guard(lock);
		resolving.swap(begun);
	}
	ErrorData first_failure;
	for (auto &entry : resolving) {
		try {
			if (commit) {
				entry.second->Commit();
			} else {
				entry.second->Rollback();
			}
		} catch (std::exception &ex) {
			if (commit && !first_failure.HasError()) {
				first_failure = ErrorData(ex);
			}
		}
	}
	if (first_failure.HasError()) {
		first_failure.Throw("virtual_catalog_bridge: commit on source failed: ");
	}
}

CrossingTransaction &TransactionFor(ClientContext &context, CrossingSource &source) {
	return CrossingTransactions::Get(context)->TransactionFor(context, source);
}

} // namespace duckdb
