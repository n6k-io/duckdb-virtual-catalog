# Implementing a source

Crossing is a library that moves part of a DuckDB query onto another database.
You implement one class against it, and your database becomes attachable:

```sql
ATTACH 'toy.example.com' AS toy (TYPE toydb);
SELECT upper(name) FROM toy.orders WHERE amt > 100 LIMIT 10;
```

DuckDB would ordinarily read every row of `orders` and do the filtering, the
`upper()` and the limit itself. Crossing hands you that work as a query instead,
and you return ten rows.

Three things to write.

1. **Describe your tables** — columns, and who may do what to which rows.
2. **Say what your database can compute.**
3. **Run the query you are handed.**

## The whole interface

```cpp
class ToyDB : public CrossingSource {
	// 1. describe
	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	unique_ptr<LogicalOperator> Plan(CrossingPlanRequest &request) override;

	// 2. compute
	CrossingVerdict AcceptsCall(const Expression &expr) override;
	CrossingVerdict AcceptsType(const LogicalType &type) override;

	// 3. run
	unique_ptr<CrossingTransaction> Begin(ClientContext &context) override;
	unique_ptr<CrossingReader> Read(CrossingTransaction &transaction, const CrossingReadQuery &query) override;
	idx_t Write(CrossingTransaction &transaction, const CrossingWriteQuery &query) override;
};
```

`Tables`, `Describe`, `Plan`, `AcceptsCall`, `AcceptsType` and `Read` must be implemented.
`Schemas` has a base implementation returning `main`. `Write` has one that
refuses, so a read-only source leaves it alone. `Begin` returns a transaction
that does nothing on commit or rollback by default: a source with no transaction
of its own has nothing to resolve.

In the examples below, anything named `Toy...` is a stand-in for your own code
talking to your own database. Crossing never sees it.

---

## 1. Describe your tables

`Schemas` and `Tables` are what DuckDB lists when someone asks what is in the
attached database. `Describe` is asked once per table, the first time someone
names one, and is where you say what the table is and who may do what to it.

What you answer is kept for the life of the attach. There is no invalidation: a
table that appears on your side afterwards is not there until the next attach,
and a policy you tighten afterwards does not reach a statement already bound.

```cpp
vector<string> ToyDB::Schemas() {
	return ToyListSchemas();
}

vector<string> ToyDB::Tables(const string &schema) {
	return ToyListTables(schema);
}

CrossingTable ToyDB::Describe(const string &schema, const string &name) {
	CrossingTable table(name);

	table.Column("id", LogicalType::INTEGER);
	table.Column("name", LogicalType::VARCHAR);
	table.Column("amt", LogicalType::INTEGER);
	table.Column("tenant", LogicalType::INTEGER);

	table.Key({"id"});

	table.Allow(CrossingVerb::SELECT);
	table.Allow(CrossingVerb::INSERT);
	table.Allow(CrossingVerb::UPDATE);

	return table;
}
```

A description is three kinds of call.

**`Column`** — the table's columns, in order, with their DuckDB types.

**`Key`** — the columns that identify a row. An update or a delete addresses rows
by it, so a table that allows either must declare one. `UniqueKey` says the same
and vouches for uniqueness: only then may an update or a delete run wholly on
the source, since nothing then counts the rows the target would have sent.

**`Allow`** — a verb you do not allow cannot happen. Not "it errors": there is no
query for it, and the attempt is refused when the statement is bound. Above,
`DELETE` is absent, so nothing can delete from this table. The enumerator is
spelled `CrossingVerb::DELETE_`, with the underscore: `DELETE` is a macro in some Windows
SDK headers.

### Row policies

Which rows a verb may reach, and which rows a write may leave behind, are yours
to enforce, and the place is `Plan`. A predicate over the table's columns only
means anything against the scan those columns come from, and that scan is the one
you build. Think of Postgres's `CREATE POLICY`: its `USING` clause goes at the
bottom of your read plan and in the `WHERE` of your update and delete; its
`WITH CHECK` clause guards what your insert and update write. `USING` alone
stops an update reaching another tenant's row but leaves it free to move one of
your rows *to* another tenant, so a writable table wants both.

Crossing never sees a policy. It sees a plan with a filter at its floor, which
nothing above can lift.

### The plans everything is built on

`Plan` is asked for one plan per verb on one table, each time a statement binds
that verb on it — a read is planned once per scan, a write once per statement.

```cpp
unique_ptr<LogicalOperator> ToyDB::Plan(CrossingPlanRequest &request) {
	switch (request.verb) {
	case CrossingVerb::SELECT:
		return ToyScanOf(request.schema, request.table);   // with your USING policy on it
	case CrossingVerb::INSERT:
	case CrossingVerb::UPDATE:
	case CrossingVerb::DELETE_:
		return ToyWriteOf(request);                       // with a seam where the rows go
	}
}
```

For `SELECT` it is your own read of the table. Everything that crosses is
stacked on top of it, and it is still there when the query comes back to you.
This is where the `USING` policy goes for a read: whatever you return is the
floor, so a filter you put here is one nothing above can lift. The node has to
emit one column per column you described, in that order.

For a write it is the statement with a hole where the rows go: a *seam*, made by
`MakeSeamNode(table_index, request.seam.types)`. `request.seam.key_columns` and
`request.seam.set_columns` say what the seam's columns are, in order: an insert's
seam is the row image in `Describe` order; an update's is the key columns then
the columns it sets; a delete's is the key columns. Put your `USING` policy in
the statement's `WHERE` and your `WITH CHECK` around what it writes.

For any verb, return null and set `request.declined` to refuse. The reason is in
the error the statement fails with, and in `EXPLAIN` for a write.

Beyond that, what a plan must be depends on what you do with the query later — a
source that renders the tree to its own query language needs only something that
names the table, because nobody executes it; a source that runs the plan it is
handed needs nodes that can actually run.

---

## 2. What ToyDB can compute

Crossing asks two questions: whether ToyDB can compute an expression, and whether
it can represent a type. Each answer is a `CrossingVerdict`: yes, or no with a
reason. Say no to either and whatever needed it stays in DuckDB; the reason is
what EXPLAIN reports when a write could not run on the source.

```cpp
CrossingVerdict ToyDB::AcceptsCall(const Expression &expr) {
	string name;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		name = expr.Cast<BoundFunctionExpression>().function.name;
		break;
	case ExpressionClass::BOUND_AGGREGATE:
		name = expr.Cast<BoundAggregateExpression>().function.name;
		break;
	default:
		return CrossingVerdict::Yes();
	}
	return ToyHasFunction(name) ? CrossingVerdict::Yes() : CrossingVerdict::No("ToyDB has no " + name);
}

CrossingVerdict ToyDB::AcceptsType(const LogicalType &type) {
	if (type.IsNumeric() || type.id() == LogicalTypeId::VARCHAR) {
		return CrossingVerdict::Yes();
	}
	return CrossingVerdict::No("ToyDB cannot hold a " + type.ToString());
}
```

`AcceptsCall` is asked about one expression at a time, and only about
kinds crossing is willing to move at all — column references, constants,
comparisons, `AND`/`OR`, operators (`IS NULL`, `IN`, `NOT`), casts, `BETWEEN`,
`CASE`, `UNNEST`, calls, and window functions. Anything else, a subquery or a
lambda say, is refused before you see it, which is why `default: Yes()` above is
safe. In practice you answer for the calls and let the rest through. A window
function arrives as a `BoundWindowExpression`: `aggregate` names the function
for `sum(...) OVER`, and the expression type does for `row_number` and its kin.

Two kinds are refused whatever you answer: an expression that is volatile, and
one that is not consistent. Evaluated once here and once on ToyDB they are two
answers to one question, and which one the query got would depend on what
crossed.

`AcceptsType` is asked about the type of every expression before it
moves. Say no and the operator carrying that type stays in DuckDB. Without it, a `STRUCT`, an
`ENUM` or an extension type reaches ToyDB inside a cast or a constant that no
other question would have caught. It is not asked about the column types you
declared in `Describe` — those you chose, so they are taken as given.

You are not asked about `LIMIT`, `GROUP BY` or joins. Crossing decides those from
the expressions they carry.

Saying yes to `upper` claims ToyDB's `upper` is DuckDB's `upper`. If it folds
case differently, answers come back wrong rather than slow. Say no when unsure.

---

## 3. Run the query

A query is DuckDB's operator tree, unoptimized. Walk it and emit ToyDB's own
query language, or serialize it and send it to something that speaks DuckDB
operators.

```cpp
class CrossingQuery {
public:
	//! The tree, printed. For logs and tests.
	string ToString() const;
	//! Which of your tables it touches. Each entry is a table name and the
	//! columns of it this query reads.
	vector<CrossingTableUse> Tables() const;

	//! What this query does. SELECT on a read, the write verb on a write.
	CrossingVerb Kind() const;

	//! The row types: what a read produces, what a write is handed.
	const vector<LogicalType> &Types() const;
};

class CrossingReadQuery : public CrossingQuery {
public:
	//! The tree.
	const LogicalOperator &Plan() const;
};

class CrossingWriteQuery : public CrossingQuery {
public:
	//! The write with its seam already filled. Run it as it is.
	const LogicalOperator &Plan() const;

	//! The columns the statement writes. An insert names every column, in the order Describe
	//! declared them. An update names the columns it sets. A delete names none.
	const vector<string> &SetColumns() const;
};
```

A read returns a reader DuckDB pulls from:

```cpp
class CrossingReader {
public:
	//! Fill `chunk` with the next rows. False when there are none left.
	bool Next(DataChunk &chunk);
};
```

```cpp
class ToyReader : public CrossingReader {
	bool Next(DataChunk &chunk) override {
		return ToyFetch(cursor, chunk);
	}
};

unique_ptr<CrossingReader> ToyDB::Read(CrossingTransaction &transaction, const CrossingReadQuery &query) {
	return make_uniq<ToyReader>(ToyOpen(transaction, query));
}
```

DuckDB asks for one chunk at a time, so a large result is never materialised in
full. When DuckDB has stopped asking — a `LIMIT` satisfied above you, or a
cancelled statement — the reader is destroyed, and releasing whatever it holds
is the whole of what you owe it.

`Read` is called once per scan DuckDB opens — so more than once for one query,
and from whichever thread opens the scan. The reader belongs to one scan and is
not shared. The chunk it fills must carry exactly `query.Types()`; anything else
fails the scan.

The source object is shared by every connection on the attach. Crossing
serialises `Describe` and `Begin` and nothing else: `Plan`, `AcceptsCall`,
`AcceptsType`, `Read` and `Write` can all run concurrently, so a source that
cannot serve two at once must lock itself.

A write runs the plan it is handed:

```cpp
idx_t ToyDB::Write(CrossingTransaction &transaction, const CrossingWriteQuery &query) {
	return ToyRun(transaction, query);
}
```

The plan is the one you returned from `Plan`, with the seam filled. When the rows
were something ToyDB could produce itself — `INSERT INTO toy.a SELECT * FROM
toy.b`, or `UPDATE toy.a SET amt = 0 WHERE tenant = 1` on a table with a
`UniqueKey` — the seam holds that read, and nothing crosses at all. Otherwise the
seam holds a `LogicalColumnDataGet` of the rows the target gathered, already in
the seam's column order and types. `Types()` is that shape.

What you return is the number of rows the statement changed, which is what DuckDB
reports back to the user. It is what ToyDB actually changed, not the number of
rows it was handed: for an update or a delete those are different numbers, and
the difference is what catches a key that identifies more than one row.

Unlike `Read`, `Write` is called once per statement.

### Transactions

`Begin` is what makes a target transaction's writes one change on the source.
Crossing calls it the first time a DuckDB transaction touches you at all — a read
as much as a write, so that a read after a write in one transaction sees it — and
hands the object back to every `Read` and `Write` in that transaction. When the
DuckDB transaction resolves, Crossing calls `Commit` or `Rollback` on it.

```cpp
class ToyTransaction : public CrossingTransaction {
	void Commit() override;
	void Rollback() override;
};

unique_ptr<CrossingTransaction> ToyDB::Begin(ClientContext &context) {
	return make_uniq<ToyTransaction>(ToyConnect());
}
```

One object per DuckDB connection: two connections on one attach never share
one. A source that cannot offer this leaves the default, which does nothing, and
will not undo a partial write when the target rolls back.

A `Commit` that throws fails the statement that committed; the target has
already committed by then, so the error is a report, not an undo. A `Rollback`
that throws is swallowed.

---

## Registering

```cpp
void ToyExtension::Load(ExtensionLoader &loader) {
	CrossingSource::Register(loader, "toydb", [](ClientContext &context, AttachInfo &info) {
		return make_uniq<ToyDB>(info.path);
	});
}
```

`"toydb"` is the name in `ATTACH ... (TYPE toydb)`. `info.path` is whatever was
attached — a host, a file, a connection string — and `info.options` carries the
rest of the parenthesised list, which is where a source that has to be authorised
rather than named reads its credentials. Your factory is called once per `ATTACH`,
and what it returns is the catalog's source for the life of the attach.

Registering installs a catalog and an optimizer pass. Crossing owns both, so
this call is the only wiring.

---

## What you get

`SELECT upper(name) FROM toy.orders WHERE amt > 100 LIMIT 10` gives you the
filter, the projection and the limit, over `orders` with your `USING` policy
beneath them. DuckDB runs nothing but the scan.

`WHERE amt > 100 AND md5(name) = '...'` gives you the `amt > 100` half only,
because `Accepts` said no to `md5`. DuckDB keeps the rest and filters what
you send back.

`toy.orders JOIN toy.customers` gives you one query over both tables, because
both are yours.

`UPDATE toy.orders SET amt = 0 WHERE id = 7` gives you an update query, keyed by
`id`, with your `USING` policy in its `WHERE` and your `WITH CHECK` guarding what
it writes.

`ORDER BY score, id LIMIT 100` gives you the sort and the limit together, and you
return a hundred rows rather than the whole table for DuckDB to sort. A sort
crosses on the same terms as everything else: `Accepts` is asked about each
sort key, and a key you refuse keeps the sort on the target.

The rows you hand back are then read in the order you produced them, because one
reader drains the scan.
