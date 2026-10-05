#pragma once

// One process-global registry type: a caller-chosen name maps to a shared_ptr keeping something
// expensive alive (a DatabaseInstance, a set of UDF names, a phantom catalog).
//
// Erase() is a member so a registration with no matching removal is a visible omission rather than
// a free function nobody thought to call, which is how a source DatabaseInstance once ended up
// pinned for the life of the process.

// duckdb::shared_ptr is its own type, not an alias for std::shared_ptr, and the two do not convert.
#include "duckdb/common/shared_ptr.hpp"

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace duckdb {
namespace vcat {

template <class T>
class SharedRegistry {
public:
	//! The entry for `key`, or null. By value, so the caller's reference survives a concurrent Erase.
	shared_ptr<T> Get(const std::string &key) const {
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = entries_.find(key);
		if (it == entries_.end()) {
			return shared_ptr<T>();
		}
		return it->second;
	}

	void Put(const std::string &key, shared_ptr<T> value) {
		std::lock_guard<std::mutex> lock(mutex_);
		entries_[key] = std::move(value);
	}

	//! False if there was nothing to remove. The entry is destroyed after the lock is released:
	//! dropping the last reference can close a DatabaseInstance, which must not run under the mutex.
	bool Erase(const std::string &key) {
		shared_ptr<T> removed;
		{
			std::lock_guard<std::mutex> lock(mutex_);
			auto it = entries_.find(key);
			if (it == entries_.end()) {
				return false;
			}
			removed = std::move(it->second);
			entries_.erase(it);
		}
		return true;
	}

private:
	mutable std::mutex mutex_;
	std::unordered_map<std::string, shared_ptr<T>> entries_;
};

} // namespace vcat
} // namespace duckdb
