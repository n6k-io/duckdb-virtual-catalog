#pragma once

// Requires -DMSGPACK_NO_BOOST (set in CMake).

#include "msgpack/object.hpp"
#include "msgpack/pack.hpp"
#include "msgpack/sbuffer.hpp"
#include "msgpack/type.hpp"
#include "msgpack/unpack.hpp"

#include <cstdint>
#include <cstring>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

namespace duckdb {
namespace n6k {

inline void EmitJsonString(std::ostream &os, const char *p, size_t n) {
	static const char hex[] = "0123456789abcdef";
	os << '"';
	for (size_t i = 0; i < n; i++) {
		unsigned char c = static_cast<unsigned char>(p[i]);
		switch (c) {
		case '"':
			os << "\\\"";
			break;
		case '\\':
			os << "\\\\";
			break;
		case '\n':
			os << "\\n";
			break;
		case '\r':
			os << "\\r";
			break;
		case '\t':
			os << "\\t";
			break;
		default:
			if (c < 0x20) {
				os << "\\u00" << hex[(c >> 4) & 0xf] << hex[c & 0xf];
			} else {
				os << static_cast<char>(c);
			}
		}
	}
	os << '"';
}

inline void EmitJson(std::ostream &os, const msgpack::object &o) {
	switch (o.type) {
	case msgpack::type::NIL:
		os << "null";
		break;
	case msgpack::type::BOOLEAN:
		os << (o.via.boolean ? "true" : "false");
		break;
	case msgpack::type::POSITIVE_INTEGER:
		os << o.via.u64;
		break;
	case msgpack::type::NEGATIVE_INTEGER:
		os << o.via.i64;
		break;
	case msgpack::type::FLOAT32:
	case msgpack::type::FLOAT64:
		os << o.via.f64;
		break;
	case msgpack::type::STR:
		EmitJsonString(os, o.via.str.ptr, o.via.str.size);
		break;
	case msgpack::type::ARRAY:
		os << '[';
		for (uint32_t i = 0; i < o.via.array.size; i++) {
			if (i) {
				os << ',';
			}
			EmitJson(os, o.via.array.ptr[i]);
		}
		os << ']';
		break;
	case msgpack::type::MAP:
		os << '{';
		for (uint32_t i = 0; i < o.via.map.size; i++) {
			if (i) {
				os << ',';
			}
			const msgpack::object_kv &kv = o.via.map.ptr[i];
			if (kv.key.type == msgpack::type::STR) {
				EmitJsonString(os, kv.key.via.str.ptr, kv.key.via.str.size);
			} else {
				std::ostringstream tmp;
				EmitJson(tmp, kv.key);
				const std::string s = tmp.str();
				EmitJsonString(os, s.data(), s.size());
			}
			os << ':';
			EmitJson(os, kv.val);
		}
		os << '}';
		break;
	default:
		os << "null";
		break;
	}
}

// JSON object of a header map's data fields — every key except the routing keys t, id, op, ns.
inline std::string HeaderPayloadJsonExcludingRouting(const msgpack::object &map) {
	std::ostringstream os;
	os << '{';
	bool first = true;
	if (map.type == msgpack::type::MAP) {
		for (uint32_t i = 0; i < map.via.map.size; i++) {
			const msgpack::object_kv &kv = map.via.map.ptr[i];
			if (kv.key.type != msgpack::type::STR) {
				continue;
			}
			const std::string key(kv.key.via.str.ptr, kv.key.via.str.size);
			if (key == "t" || key == "id" || key == "op" || key == "ns") {
				continue;
			}
			if (!first) {
				os << ',';
			}
			first = false;
			EmitJsonString(os, key.data(), key.size());
			os << ':';
			EmitJson(os, kv.val);
		}
	}
	os << '}';
	return os.str();
}

inline const msgpack::object *FindKey(const msgpack::object &map, const char *key) {
	if (map.type != msgpack::type::MAP) {
		return nullptr;
	}
	const size_t klen = std::char_traits<char>::length(key);
	for (uint32_t i = 0; i < map.via.map.size; i++) {
		const msgpack::object_kv &kv = map.via.map.ptr[i];
		if (kv.key.type == msgpack::type::STR && kv.key.via.str.size == klen &&
		    std::memcmp(kv.key.via.str.ptr, key, klen) == 0) {
			return &kv.val;
		}
	}
	return nullptr;
}

inline int64_t GetInt(const msgpack::object &map, const char *key, int64_t fallback = 0) {
	const msgpack::object *v = FindKey(map, key);
	if (!v) {
		return fallback;
	}
	if (v->type == msgpack::type::POSITIVE_INTEGER) {
		return static_cast<int64_t>(v->via.u64);
	}
	if (v->type == msgpack::type::NEGATIVE_INTEGER) {
		return v->via.i64;
	}
	return fallback;
}

inline bool HasKey(const msgpack::object &map, const char *key) {
	return FindKey(map, key) != nullptr;
}

inline bool GetBool(const msgpack::object &map, const char *key, bool fallback = false) {
	const msgpack::object *v = FindKey(map, key);
	return (v && v->type == msgpack::type::BOOLEAN) ? v->via.boolean : fallback;
}

inline std::string GetStr(const msgpack::object &map, const char *key) {
	const msgpack::object *v = FindKey(map, key);
	if (v && v->type == msgpack::type::STR) {
		return std::string(v->via.str.ptr, v->via.str.size);
	}
	return std::string();
}

inline std::vector<std::string> GetStrArray(const msgpack::object &map, const char *key) {
	std::vector<std::string> out;
	const msgpack::object *v = FindKey(map, key);
	if (!v || v->type != msgpack::type::ARRAY) {
		return out;
	}
	out.reserve(v->via.array.size);
	for (uint32_t i = 0; i < v->via.array.size; i++) {
		const msgpack::object &item = v->via.array.ptr[i];
		if (item.type == msgpack::type::STR) {
			out.emplace_back(item.via.str.ptr, item.via.str.size);
		}
	}
	return out;
}

inline void PackString(msgpack::packer<msgpack::sbuffer> &pk, const char *k) {
	pk.pack(std::string(k));
}

} // namespace n6k
} // namespace duckdb
