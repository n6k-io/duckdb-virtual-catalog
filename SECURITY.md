# Threat model

## Trusted

- **Source instance.** Declares the policy and the context. Every policy and context function
  refuses a connection whose `DatabaseInstance` is not the one that called
  `vcatbridge_register_source`. The policy is the owner's input: a primary key's `columns_from_query`
  and `verify_unique` run on the source as written.
- **Provider host.** Registers its UDFs on the same `DatabaseInstance` as the catalog, so it
  already owns everything there.

## Attacker

Every attack arrives as SQL on a connection. Host APIs (Python `create_function`, the C++ API,
loading a native library) belong to whoever owns the process and are out of scope.

- **Target instance.** Runs arbitrary SQL on any connection to the target.
- **Any other instance in the process.** Runs arbitrary SQL and may know a bridge id.
- **Arrow frames.** Malformed or hostile frames are attacker input.
- **Provider SQL caller.** Errors must not become an existence oracle over hidden rows.

## Boundary

- **One `DatabaseInstance` per side.** A same-instance bridge is not a security boundary: the
  target can read the source catalog directly.
- **Pushed calls.** A call crosses to the source only when it is on the bridge's function
  whitelist: the built-in list, as the owner changed it. Built-ins that read the instance they run
  in, such as `current_setting`, are not on the built-in list and stay on the target.
- **`vcatbridge_lockdown`** hardens the target instance: no files, network, extensions or
  config changes; memory and thread caps.

## Assets the attacker must not reach

- Ungranted tables, rows and columns.
- Grants, context and keys.
- Source-side execution of target-authored expressions.
- The host filesystem and network.
- The process: memory safety and hangs.

## User error

A mistake by the trusted source owner. It is not a vulnerability. The bridge should fail with a
clear error where it can, but it does not have to stay safe. Examples:

- Registering a host-API function under a built-in's name on the source.
- Adding an unsafe function to the whitelist.
- Granting a policy that exposes more than intended.

## Accepted leaks

- **DDL access can reveal ungranted table names.**
- **A `foreign_keys` entry reveals which parent keys exist.** An `insert` or `update` succeeds
  or fails depending on whether the key exists in the parent table, policy-hidden rows included.
- **A foreign key from an ungranted table names the referencing column, not the referencing table.**
  A `delete`, or an `update` of the referenced key, fails while an ungranted row references that
  key. The error names the ungranted table's foreign-key column (e.g. `uid`), but not the table
  (e.g. `secret_payroll`).
