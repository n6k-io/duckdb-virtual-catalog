#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"
#include <string>

namespace duckdb {

static void AdoptIpcBufferAsArrayStream(uint8_t *buf, int64_t len, ArrowArrayStream *out) {
	struct ArrowBuffer arrow_buf;
	ArrowBufferInit(&arrow_buf);
	arrow_buf.data = buf;
	arrow_buf.size_bytes = len;
	arrow_buf.capacity_bytes = len;

	struct ArrowIpcInputStream input_stream;
	auto rc = ArrowIpcInputStreamInitBuffer(&input_stream, &arrow_buf);
	if (rc != NANOARROW_OK) {
		free(buf);
		throw IOException("n6k: ArrowIpcInputStreamInitBuffer failed");
	}

	rc = ArrowIpcArrayStreamReaderInit(out, &input_stream, nullptr);
	if (rc != NANOARROW_OK) {
		if (input_stream.release) {
			input_stream.release(&input_stream);
		}
		throw IOException("n6k: ArrowIpcArrayStreamReaderInit failed");
	}
}

static void SerializeChunksToArrowIPC(ClientContext &context, const vector<LogicalType> &types,
                                      const vector<string> &col_names, vector<unique_ptr<DataChunk>> &chunks,
                                      ArrowBuffer *out_buf) {
	auto client_props = context.GetClientProperties();

	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, types, col_names, client_props);

	ArrowIpcOutputStream output_stream;
	ArrowIpcOutputStreamInitBuffer(&output_stream, out_buf);

	ArrowIpcWriter writer;
	ArrowIpcWriterInit(&writer, &output_stream);
	ArrowIpcWriterWriteSchema(&writer, &arrow_schema, nullptr);

	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> empty_extensions;
	for (auto &chunk : chunks) {
		if (chunk->size() == 0) {
			continue;
		}
		ArrowArray arrow_array;
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, client_props, empty_extensions);

		ArrowArrayView array_view;
		ArrowArrayViewInitFromSchema(&array_view, &arrow_schema, nullptr);
		ArrowArrayViewSetArray(&array_view, &arrow_array, nullptr);
		ArrowIpcWriterWriteArrayView(&writer, &array_view, nullptr);
		ArrowArrayViewReset(&array_view);

		if (arrow_array.release) {
			arrow_array.release(&arrow_array);
		}
	}

	ArrowIpcWriterReset(&writer);
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}
}

} // namespace duckdb
