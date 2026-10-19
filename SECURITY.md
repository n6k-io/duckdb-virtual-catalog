# Threat model

## Trusted

- **Source instance.** Declares grants, context and keys. Every grant and context function
  refuses a connection whose `DatabaseInstance` is not the one that called
  `bridge_register_source`.
- **Provider host.** Registers its UDFs on the same `DatabaseInstance` as the catalog, so it
  already owns everything there.

## Attacker

- **Target instance.** Runs arbitrary SQL.
- **Any other instance in the process.** May know a bridge id.
- **Arrow frames.** Malformed or hostile frames are attacker input.
- **Provider SQL caller.** Errors must not become an existence oracle over hidden rows.

## Boundary

- **One `DatabaseInstance` per side.** A same-instance bridge is not a security boundary: the
  target can read the source catalog directly.
- **`virtual_catalog_lockdown`** hardens the target instance: no files, network, extensions or
  config changes; memory and thread caps.

## Assets the attacker must not reach

- Ungranted tables, rows and columns.
- Grants, context and keys.
- Source-side execution of target-authored expressions.
- The host filesystem and network.
- The process: memory safety and hangs.
