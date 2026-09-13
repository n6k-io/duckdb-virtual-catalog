#include "frame_codec.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace n6k {

void ParseFrame(const std::string &frame, msgpack::object_handle &oh, const char *&body, size_t &body_len) {
	size_t off = 0;
	try {
		oh = msgpack::unpack(frame.data(), frame.size(), off);
	} catch (const std::exception &e) {
		throw InvalidInputException("n6k: malformed frame from client (msgpack): %s", e.what());
	}
	if (oh.get().type != msgpack::type::MAP) {
		throw InvalidInputException("n6k: malformed frame from client: header is not a msgpack map");
	}
	body = frame.data() + off;
	body_len = frame.size() - off;
}

namespace {

std::string PackedHeader(msgpack::sbuffer &sb) {
	return std::string(sb.data(), sb.size());
}

// Connection-scoped frames (HELLO_ACK/HELLO_ERR) carry `ns` but no `id`.
void PackNsInto(msgpack::packer<msgpack::sbuffer> &pk, int64_t ns) {
	if (ns != 0) {
		PackString(pk, "ns");
		pk.pack(ns);
	}
}

// Same accounting as NsKeyCount(FrameTarget), for the frames that carry `ns` without an `id`
// (HELLO_ACK, HELLO_ERR, PUSH).
uint32_t NsKeyCount(int64_t ns) {
	return ns != 0 ? 1u : 0u;
}

} // namespace

std::string PackHelloAck(int64_t ns) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(5 + NsKeyCount(ns));
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::HELLO_ACK));
	PackString(pk, "protocol_version");
	pk.pack(static_cast<int>(N6K_PROTOCOL_VERSION));
	PackString(pk, "max_concurrent_reqs");
	pk.pack(static_cast<int>(MAX_CONCURRENT_REQS));
	PackString(pk, "default_batch_credits");
	pk.pack(static_cast<int>(DEFAULT_BATCH_CREDITS));
	// A capability must be advertised here, not discovered by a failing request: the client's
	// aggregate-pushdown optimizer rule rewrites the plan before any request is sent and has no
	// way to fall back once it has.
	PackString(pk, "capabilities");
	pk.pack_array(1);
	pk.pack(std::string(CAP_AGGREGATE_PUSHDOWN));
	PackNsInto(pk, ns);
	return PackedHeader(sb);
}

std::string PackHelloErr(int64_t ns, const std::string &type, const std::string &message) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(ns));
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::HELLO_ERR));
	PackString(pk, "exception_type");
	pk.pack(type);
	PackString(pk, "exception_message");
	pk.pack(message);
	PackNsInto(pk, ns);
	return PackedHeader(sb);
}

std::string PackPong(uint32_t req_id) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(2);
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::PONG));
	PackString(pk, "id");
	pk.pack(req_id);
	return PackedHeader(sb);
}

std::string PackPing(uint32_t ping_id) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(2);
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::PING));
	PackString(pk, "id");
	pk.pack(ping_id);
	return PackedHeader(sb);
}

std::string PackPushInvalidate(int64_t ns, const std::vector<std::string> &schemas) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(ns));
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::PUSH));
	PackString(pk, "op");
	pk.pack(static_cast<int>(OP_CATALOG_INVALIDATED));
	PackString(pk, "schemas");
	pk.pack_array(static_cast<uint32_t>(schemas.size()));
	for (auto &s : schemas) {
		pk.pack(s);
	}
	PackNsInto(pk, ns);
	return PackedHeader(sb);
}

std::string PackErr(const FrameTarget &target, const std::string &type, const std::string &message, bool retriable) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map((retriable ? 5 : 4) + NsKeyCount(target));
	PackTIdInto(pk, FrameType::ERR, target);
	PackString(pk, "exception_type");
	pk.pack(type);
	PackString(pk, "exception_message");
	pk.pack(message);
	if (retriable) {
		PackString(pk, "retriable");
		pk.pack(true);
	}
	return PackedHeader(sb);
}

std::string PackRespSchema(const FrameTarget &target, const std::string &arrow) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(2 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_SCHEMA, target);
	std::string frame = PackedHeader(sb);
	frame.append(arrow);
	return frame;
}

std::string PackRespChunkArrow(const FrameTarget &target, const std::string &arrow) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_CHUNK, target);
	PackString(pk, "arrow");
	pk.pack(true);
	std::string frame = PackedHeader(sb);
	frame.append(arrow);
	return frame;
}

std::string PackRespEnd(const FrameTarget &target) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(2 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_END, target);
	return PackedHeader(sb);
}

std::string PackRespEndRowcount(const FrameTarget &target, int64_t rowcount) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_END, target);
	PackString(pk, "rowcount");
	pk.pack(rowcount);
	return PackedHeader(sb);
}

std::string PackRespEndRowcountNull(const FrameTarget &target) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_END, target);
	PackString(pk, "rowcount");
	pk.pack_nil();
	return PackedHeader(sb);
}

std::string PackRespEndCancelled(const FrameTarget &target) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_END, target);
	PackString(pk, "cancelled");
	pk.pack(true);
	return PackedHeader(sb);
}

std::string PackRespEndOk(const FrameTarget &target) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_END, target);
	PackString(pk, "ok");
	pk.pack(true);
	return PackedHeader(sb);
}

} // namespace n6k
} // namespace duckdb
