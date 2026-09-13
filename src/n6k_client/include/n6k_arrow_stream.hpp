#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_fetch.hpp"
#include "n6k_pushdown.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <memory>

namespace duckdb {

struct N6kStreamData {
	ArrowArrayStream stream;
	bool consumed;

	N6kStreamData() : consumed(false) {
		stream.release = nullptr;
	}

	~N6kStreamData() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

struct N6kScanFunctionData : public ArrowScanFunctionData {
	unique_ptr<N6kStreamData> owned_stream_data;

	N6kScanFunctionData(stream_factory_produce_t producer, unique_ptr<N6kStreamData> stream_data)
	    : ArrowScanFunctionData(producer, reinterpret_cast<uintptr_t>(stream_data.get())),
	      owned_stream_data(std::move(stream_data)) {
		// Full materialization at bind can't honor projected column_ids; disable pushdown (must match table fns).
		projection_pushdown_enabled = false;
	}
};

static unique_ptr<ArrowArrayStreamWrapper> N6kProduceStream(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
	auto *stream_data = reinterpret_cast<N6kStreamData *>(factory_ptr);

	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	if (stream_data->consumed) {
		wrapper->arrow_array_stream.release = nullptr;
		return wrapper;
	}

	wrapper->arrow_array_stream = stream_data->stream;
	stream_data->stream.release = nullptr;
	stream_data->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

// Lazy: defers scan so projection/filter pushdown builds into the WS request; requires sess != nullptr.
struct N6kLazyStreamData {
	string schema_name;
	string table_name;
	std::shared_ptr<CatalogSession> session;
	ClientContext &context;
	ArrowArrayStream stream;
	ArrowSchema *schema;
	bool fetched;
	bool consumed;

	N6kLazyStreamData(ClientContext &ctx, std::shared_ptr<CatalogSession> sess, string schema_n, string table_n)
	    : schema_name(std::move(schema_n)), table_name(std::move(table_n)), session(std::move(sess)), context(ctx),
	      schema(nullptr), fetched(false), consumed(false) {
		stream.release = nullptr;
	}

	~N6kLazyStreamData() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

struct N6kLazyScanFunctionData : public ArrowScanFunctionData {
	unique_ptr<N6kLazyStreamData> owned_stream_data;
	// Keeps the schema ArrowSchema pointer valid.
	unique_ptr<N6kStreamData> owned_schema_stream;
	TableCatalogEntry *table = nullptr;

	N6kLazyScanFunctionData(stream_factory_produce_t producer, unique_ptr<N6kLazyStreamData> stream_data,
	                        unique_ptr<N6kStreamData> schema_stream)
	    : ArrowScanFunctionData(producer, reinterpret_cast<uintptr_t>(stream_data.get())),
	      owned_stream_data(std::move(stream_data)), owned_schema_stream(std::move(schema_stream)) {
	}
};

static unique_ptr<ArrowArrayStreamWrapper> N6kProduceLazyStream(uintptr_t factory_ptr,
                                                                ArrowStreamParameters &parameters) {
	auto *stream_data = reinterpret_cast<N6kLazyStreamData *>(factory_ptr);

	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	if (stream_data->consumed) {
		wrapper->arrow_array_stream.release = nullptr;
		return wrapper;
	}

	if (!stream_data->fetched) {
		auto query = BuildScanUrlQueryString(parameters, *stream_data->schema);
		stream_data->session->Scan(stream_data->schema_name, stream_data->table_name, query, &stream_data->stream,
		                           [stream_data]() { return stream_data->context.IsInterrupted(); });
		stream_data->fetched = true;
	}

	wrapper->arrow_array_stream = stream_data->stream;
	stream_data->stream.release = nullptr;
	stream_data->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

} // namespace duckdb
