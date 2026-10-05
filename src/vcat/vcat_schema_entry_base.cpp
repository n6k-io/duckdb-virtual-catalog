#include "vcat_schema_entry_base.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

VirtualCatalogSchemaInfoHolder::VirtualCatalogSchemaInfoHolder(const SchemaCatalogEntry &src) {
	info.schema = src.name;
	info.internal = src.internal;
	info.comment = src.comment;
	info.tags = src.tags;
}

VirtualCatalogSchemaEntryBase::VirtualCatalogSchemaEntryBase(Catalog &catalog, SchemaCatalogEntry &target_schema_p)
    : VirtualCatalogSchemaInfoHolder(target_schema_p), SchemaCatalogEntry(catalog, info),
      target_schema(target_schema_p) {
}

bool VirtualCatalogSchemaEntryBase::HasNativeEntry(CatalogTransaction transaction, CatalogType type,
                                                   const string &entry_name) {
	EntryLookupInfo lookup(type, entry_name);
	return target_schema.LookupEntry(transaction, lookup) != nullptr;
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateTable(CatalogTransaction transaction,
                                                                      BoundCreateTableInfo &info) {
	if (!HasNativeEntry(transaction, CatalogType::TABLE_ENTRY, info.Base().table)) {
		bool handled;
		auto created = TryCreateExtensionTable(transaction, info, handled);
		if (handled) {
			return created;
		}
	}
	return target_schema.CreateTable(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateFunction(CatalogTransaction transaction,
                                                                         CreateFunctionInfo &info) {
	return target_schema.CreateFunction(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateView(CatalogTransaction transaction,
                                                                     CreateViewInfo &info) {
	return target_schema.CreateView(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateIndex(CatalogTransaction transaction,
                                                                      CreateIndexInfo &info, TableCatalogEntry &table) {
	return target_schema.CreateIndex(transaction, info, table);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateSequence(CatalogTransaction transaction,
                                                                         CreateSequenceInfo &info) {
	return target_schema.CreateSequence(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateTableFunction(CatalogTransaction transaction,
                                                                              CreateTableFunctionInfo &info) {
	return target_schema.CreateTableFunction(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateCopyFunction(CatalogTransaction transaction,
                                                                             CreateCopyFunctionInfo &info) {
	return target_schema.CreateCopyFunction(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreatePragmaFunction(CatalogTransaction transaction,
                                                                               CreatePragmaFunctionInfo &info) {
	return target_schema.CreatePragmaFunction(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateCollation(CatalogTransaction transaction,
                                                                          CreateCollationInfo &info) {
	return target_schema.CreateCollation(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::CreateType(CatalogTransaction transaction,
                                                                     CreateTypeInfo &info) {
	return target_schema.CreateType(transaction, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntryBase::LookupEntry(CatalogTransaction transaction,
                                                                      const EntryLookupInfo &lookup_info) {
	// Native wins a name collision; the extension only answers for names the wrapped schema does not
	// have.
	auto native = target_schema.LookupEntry(transaction, lookup_info);
	if (native) {
		return native;
	}

	auto entry_type = lookup_info.GetCatalogType();
	if (entry_type != CatalogType::TABLE_ENTRY && entry_type != CatalogType::VIEW_ENTRY) {
		return nullptr;
	}
	return LookupExtensionEntry(transaction, lookup_info.GetEntryName());
}

static case_insensitive_set_t CollectNativeNames(SchemaCatalogEntry &target, optional_ptr<ClientContext> context,
                                                 CatalogType type) {
	case_insensitive_set_t out;
	auto visit = [&](CatalogEntry &e) {
		out.insert(e.name);
	};
	if (context) {
		target.Scan(*context, type, visit);
	} else {
		target.Scan(type, visit);
	}
	return out;
}

void VirtualCatalogSchemaEntryBase::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type == CatalogType::TABLE_ENTRY || info.type == CatalogType::VIEW_ENTRY) {
		auto transaction = CatalogTransaction(catalog, context);
		if (!HasNativeEntry(transaction, info.type, info.name)) {
			if (TryDropExtensionEntry(context, info)) {
				return;
			}
			ThrowIfExtensionOwnedOnDrop(info.name);
		}
	}
	target_schema.DropEntry(context, info);
}

void VirtualCatalogSchemaEntryBase::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type == AlterType::ALTER_TABLE && !HasNativeEntry(transaction, CatalogType::TABLE_ENTRY, info.name)) {
		if (TryAlterExtensionEntry(transaction, info.Cast<AlterTableInfo>())) {
			return;
		}
	}
	// A comment is the target's own note about the entry, not a change to what backs it, so an
	// extension entry takes it the same way a native one does. Native still wins the name, as it
	// does in LookupEntry.
	if (info.type == AlterType::SET_COMMENT) {
		auto &comment = info.Cast<SetCommentInfo>();
		if (comment.entry_catalog_type == CatalogType::TABLE_ENTRY ||
		    comment.entry_catalog_type == CatalogType::VIEW_ENTRY) {
			EntryLookupInfo lookup(comment.entry_catalog_type, info.name);
			if (!target_schema.LookupEntry(transaction, lookup)) {
				if (auto *entry = LookupExtensionEntry(transaction, info.name)) {
					entry->comment = comment.comment_value;
					return;
				}
			}
		}
	}
	target_schema.Alter(transaction, info);
}

void VirtualCatalogSchemaEntryBase::Scan(ClientContext &context, CatalogType type,
                                         const std::function<void(CatalogEntry &)> &callback) {
	target_schema.Scan(context, type, callback);
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
		return;
	}
	auto seen = CollectNativeNames(target_schema, &context, type);
	ScanExtensionEntries(&context, type, seen, callback);
}

void VirtualCatalogSchemaEntryBase::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	target_schema.Scan(type, callback);
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
		return;
	}
	auto seen = CollectNativeNames(target_schema, nullptr, type);
	ScanExtensionEntries(nullptr, type, seen, callback);
}

void VirtualCatalogSchemaEntryBase::CollectPermissions(ClientContext &context, optional_ptr<const string> table_filter,
                                                       vector<TablePermissionRow> &out) {
	case_insensitive_set_t seen;
	CollectNativeSchemaPermissions(context, target_schema, name, table_filter, seen, out);
	CollectExtensionPermissions(context, table_filter, seen, out);
}

} // namespace duckdb
