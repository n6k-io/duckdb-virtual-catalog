#pragma once

// One process-global registry type, so the several things that need one stop each inventing their
// own mutex + map + lifetime discipline.
//
// The shape they all share: a name chosen by the caller maps to a shared_ptr that keeps something
// expensive alive (a DatabaseInstance, a set of UDF names, a phantom catalog). What they did NOT
// share was teardown — one exposed an unregister call, one only ever grew — which is how a bridge
// came to pin its source DatabaseInstance for the life of the process. Erase() is part of the type
// here so a registration without a matching removal is a visible omission rather than a missing
// free function nobody called.

// duckdb::shared_ptr is its own type, not an alias for std::shared_ptr, and the two do not convert.
#include "duckdb/common/shared_ptr.hpp"

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace duckdb {
namespace vcat {

//! `PointerT` defaults to duckdb::shared_ptr because that is what catalog-side code holds; pass
//! std::shared_ptr<T> explicitly for a registry whose values come from the std side.
template <class T, class PointerT = shared_ptr<T>>
class SharedRegistry {
public:
	//! The entry for `key`, or null. Returned by value: the caller then holds its own reference and
	//! is safe against a concurrent Erase.
	PointerT Get(const std::string &key) const {
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = entries_.find(key);
		if (it == entries_.end()) {
			return PointerT();
		}
		return it->second;
	}

	bool Contains(const std::string &key) const {
		std::lock_guard<std::mutex> lock(mutex_);
		return entries_.find(key) != entries_.end();
	}

	//! Claim `key`. False (and no write) if it is already taken — the check and the insert are one
	//! critical section, so two callers racing for the same name cannot both win.
	bool Insert(const std::string &key, PointerT value) {
		std::lock_guard<std::mutex> lock(mutex_);
		return entries_.emplace(key, std::move(value)).second;
	}

	//! Register `key`, replacing any existing entry. For registries where re-registration is the
	//! documented way to update; use Insert where a duplicate is an error.
	void Put(const std::string &key, PointerT value) {
		std::lock_guard<std::mutex> lock(mutex_);
		entries_[key] = std::move(value);
	}

	//! False if there was nothing to remove, which callers surface as "no such registration".
	//! The removed entry is destroyed after the lock is released: dropping the last reference can
	//! run arbitrary teardown (closing a DatabaseInstance), and that must not happen while holding a
	//! registry-wide mutex.
	bool Erase(const std::string &key) {
		PointerT removed;
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
	std::unordered_map<std::string, PointerT> entries_;
};

} // namespace vcat
} // namespace duckdb
