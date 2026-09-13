#include "crossing.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

const char *CrossingVerbName(CrossingVerb verb) {
	switch (verb) {
	case CrossingVerb::SELECT:
		return "select";
	case CrossingVerb::INSERT:
		return "insert";
	case CrossingVerb::UPDATE:
		return "update";
	case CrossingVerb::DELETE_:
		return "delete";
	default:
		throw InternalException("crossing: unknown verb %d", static_cast<int>(verb));
	}
}

vector<string> CrossingSource::Schemas() {
	return {"main"};
}

idx_t CrossingSource::Write(CrossingTransaction &transaction, const CrossingWriteQuery &query) {
	throw NotImplementedException("crossing: this source is read-only");
}

unique_ptr<CrossingTransaction> CrossingSource::Begin(ClientContext &context) {
	return make_uniq<CrossingTransaction>();
}

void CrossingTransaction::Commit() {
}

void CrossingTransaction::Rollback() {
}

} // namespace duckdb
