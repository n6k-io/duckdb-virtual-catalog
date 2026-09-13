#pragma once

// Push-based Arrow-IPC frame decoder. Inverts nanoarrow's pull-based stream reader
// (ArrowIpcArrayStreamReaderInit), whose read() callback blocks with no "resume me" return. Nothing
// here blocks, so the caller keeps control of its thread between frames.
//
// One payload may carry several IPC messages: a producer that flushes its schema lazily prepends the
// schema message to the first batch payload.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

// C Data Interface structs (defined identically by nanoarrow and duckdb under the shared
// ARROW_C_DATA_INTERFACE guard); forward-declared here so the header stays nanoarrow-free.
struct ArrowArray;
struct ArrowSchema;

namespace duckdb {
namespace vcat {

class ArrowFrameDecoder {
public:
	ArrowFrameDecoder();
	~ArrowFrameDecoder();
	ArrowFrameDecoder(const ArrowFrameDecoder &) = delete;
	ArrowFrameDecoder &operator=(const ArrowFrameDecoder &) = delete;

	// An empty payload (a lazily-flushed schema) is a no-op. Returns false and records
	// ErrorMessage() on any nanoarrow failure; the decoder must not be used after that.
	bool PushFrame(const uint8_t *data, size_t len);
	bool PushFrame(const std::string &payload) {
		return PushFrame(reinterpret_cast<const uint8_t *>(payload.data()), payload.size());
	}

	// Oldest batch first; the caller then owns it and must release it. False when none is queued.
	bool TryPopBatch(ArrowArray *out);

	bool HasBatch() const;
	bool HasSchema() const;
	bool SawEndOfStream() const;

	// The decoded stream schema, valid once HasSchema(); nullptr before. Owned by the decoder.
	const ArrowSchema *GetSchema() const;

	// Non-empty after PushFrame() returns false.
	const std::string &ErrorMessage() const;

private:
	struct State;
	std::unique_ptr<State> state_;
};

} // namespace vcat
} // namespace duckdb
