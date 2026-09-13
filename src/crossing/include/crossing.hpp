#pragma once

// The API a source implements. See IMPLEMENTING.md.
//
// The engine listed in CROSSING_ENGINE_SOURCE_NAMES neither parses, binds, nor reaches for a
// catalog, which is what lets crossing_unittest build it alone and test it on plans built by hand.
//
// Vocabulary. One word per concept, everywhere:
//   crossing   the library, and a scan or write that runs on a source
//   source     the database a CrossingSource fronts; the target is DuckDB
//   fragment   the plan a crossing carries to the source
//   floor      the source's own scan at the bottom of a read fragment; sealed once built
//   seam       the hole in a write fragment where the rows go
//   fence      the logical operator standing in for a write the source runs alone
//   feed       what a fence takes rows from
//   fold       moving a read subtree into its fragment
//   fill       moving a feed into a seam
//   frozen     a fragment the pass must leave as bound
//   verdict    a yes, or a no with a reason
//   obstacle   why a write cannot run wholly on its source
//   declined   why a source returned no plan

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/constraint.hpp"

#include <functional>

namespace duckdb {

class ExtensionLoader;
class Expression;
class LogicalOperator;
class ClientContext;
struct AttachInfo;

//! DELETE_ trails an underscore: DELETE is a macro in some Windows SDK headers.
enum class CrossingVerb : uint8_t { SELECT = 0, INSERT = 1, UPDATE = 2, DELETE_ = 3 };

static constexpr idx_t CROSSING_VERB_COUNT = 4;

//! Lowercase wire spelling, as reported by the permissions function.
const char *CrossingVerbName(CrossingVerb verb);

class CrossingTable {
public:
	explicit CrossingTable(string name_p) : name(std::move(name_p)) {
	}

	//! Source order. That order is what every column position in a query means.
	void Column(string column_name, LogicalType type) {
		column_names.push_back(std::move(column_name));
		column_types.push_back(std::move(type));
	}

	//! A verb not allowed here is refused at bind time; the source is never asked about it.
	void Allow(CrossingVerb verb) {
		verbs |= static_cast<uint8_t>(1u << static_cast<uint8_t>(verb));
	}

	void Key(vector<string> columns) {
		key = std::move(columns);
		key_unique = false;
	}

	//! A key the source vouches is unique. Only then may a keyed write run wholly on the source,
	//! since nothing counts what the target sent.
	void UniqueKey(vector<string> columns) {
		key = std::move(columns);
		key_unique = true;
	}

	//! A constraint the source enforces. Declaring one your source does not enforce claims a
	//! guarantee nothing keeps, so only pass what is real.
	void Constraint(unique_ptr<duckdb::Constraint> constraint) {
		constraints.push_back(std::move(constraint));
	}

	const string &Name() const {
		return name;
	}
	const vector<string> &ColumnNames() const {
		return column_names;
	}
	const vector<LogicalType> &ColumnTypes() const {
		return column_types;
	}
	bool Allows(CrossingVerb verb) const {
		return (verbs & static_cast<uint8_t>(1u << static_cast<uint8_t>(verb))) != 0;
	}
	const vector<string> &KeyColumns() const {
		return key;
	}
	bool KeyIsUnique() const {
		return key_unique;
	}
	const vector<unique_ptr<duckdb::Constraint>> &Constraints() const {
		return constraints;
	}

private:
	string name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! Bit per verb: 1u << uint8_t(verb).
	uint8_t verbs = 0;
	vector<string> key;
	bool key_unique = false;
	vector<unique_ptr<duckdb::Constraint>> constraints;
};

struct CrossingTableUse {
	string schema;
	string table;
	//! Indexes into the table's columns, in the order Describe declared them.
	vector<idx_t> columns;
};

//! What a read and a write both carry.
class CrossingQuery {
public:
	virtual ~CrossingQuery() {
	}

	virtual string ToString() const = 0;

	virtual vector<CrossingTableUse> Tables() const = 0;

	//! SELECT on a read, the write verb on a write.
	virtual CrossingVerb Kind() const = 0;

	//! The row types: what a read produces, what a write is handed.
	virtual const vector<LogicalType> &Types() const = 0;
};

class CrossingReadQuery : public CrossingQuery {
public:
	//! Unoptimized, borrowed for the call, and not yours to consume.
	virtual const LogicalOperator &Plan() const = 0;
};

class CrossingWriteQuery : public CrossingQuery {
public:
	//! The write with its seam already filled: by a plan of yours when the rows come from this
	//! source, otherwise by the rows the target gathered. Run it as it is.
	virtual const LogicalOperator &Plan() const = 0;

	//! The columns the statement writes. An insert names every column of the table, in the order
	//! Describe declared them. An update names the columns it sets. A delete names none.
	virtual const vector<string> &SetColumns() const = 0;
};

struct CrossingVerdict {
	bool ok = true;
	string reason;

	static CrossingVerdict Yes() {
		return CrossingVerdict();
	}
	static CrossingVerdict No(string reason) {
		CrossingVerdict verdict;
		verdict.ok = false;
		verdict.reason = std::move(reason);
		return verdict;
	}
};

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types);

class CrossingReader {
public:
	virtual ~CrossingReader() {
	}

	virtual bool Next(DataChunk &chunk) = 0;
};

class CrossingTransaction {
public:
	virtual ~CrossingTransaction() {
	}

	virtual void Commit();
	virtual void Rollback();
};

struct CrossingSeam {
	vector<string> key_columns;
	vector<string> set_columns;
	vector<LogicalType> types;
};

//! What the source is asked to plan. SELECT wants a scan of the table emitting one column per
//! column Describe declared, in that order. A write wants the statement with a seam node
//! (MakeSeamNode) where the rows go.
struct CrossingPlanRequest {
	CrossingVerb verb;
	string schema;
	string table;
	CrossingSeam seam;

	string declined;
};

class CrossingSource {
public:
	virtual ~CrossingSource() {
	}

	virtual vector<string> Schemas();

	virtual vector<string> Tables(const string &schema) = 0;

	virtual CrossingTable Describe(const string &schema, const string &name) = 0;

	virtual unique_ptr<LogicalOperator> Plan(CrossingPlanRequest &request) = 0;

	virtual CrossingVerdict AcceptsCall(const Expression &expr) = 0;

	virtual CrossingVerdict AcceptsType(const LogicalType &type) = 0;

	virtual unique_ptr<CrossingTransaction> Begin(ClientContext &context);

	virtual unique_ptr<CrossingReader> Read(CrossingTransaction &transaction, const CrossingReadQuery &query) = 0;

	//! Returns the number of rows the statement changed on the source.
	virtual idx_t Write(CrossingTransaction &transaction, const CrossingWriteQuery &query);

	using Factory = std::function<unique_ptr<CrossingSource>(ClientContext &context, AttachInfo &info)>;

	static void Register(ExtensionLoader &loader, const string &type, Factory factory);
};

} // namespace duckdb
