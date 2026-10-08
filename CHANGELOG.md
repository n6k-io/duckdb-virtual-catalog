## virtual_catalog_bridge

- A grant can name a source view as well as a table. A view can only be granted `select`; `bridge_policy` refuses any other verb.

## virtual_catalog_provider

- A provider is scoped to the database it is attached in. The same catalog name in two databases no longer collides.
- Fixed: closing a database with a provider registered no longer keeps it open, so its file can be reopened.
