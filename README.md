# fridica-store-sqlite

Fridica's state on SQLite: the store thread, the schema and its migrations, weekly
archives, and the replay ledger. [Fridica](https://github.com/chengcli/fridica) uses it
through `fridica::store`; the types it stores come from
[fridica-core](https://github.com/chengcli/fridica-core).

This is the first storage backend. The plan
([fridica#117](https://github.com/chengcli/fridica/issues/117)) moves every query behind
storage traits in fridica-core (`fridica_core::store`), implemented here, so another
backend can implement the same traits and pass the same conformance suite.

## Conformance

The store passes fridica-core's storage conformance suite
(`fridica_core::store::conformance`): `tests/conformance.rs` runs every check against a
fresh `Store::open` in a temporary directory, and `tests/contract.rs` keeps the few
checks that need SQL (a fixture no trait writes, or a stored column no trait reads
back), each with its reason. Run both with:

```sh
cargo test --test conformance --test contract
```

## Modules

- `Store` (the crate root): one dedicated thread owns the connection (WAL, foreign
  keys, a 5 s busy timeout); callers run closures on it with `Store::call`. The file
  is private (0600) and locked against a second daemon.
- The storage contract: `Store` implements `fridica_core::store::Store`, and a unit
  of work is one transaction; `Sqlite` implements every area trait over a connection,
  which is also how a host reaches them from inside its own queries until they all
  move here. The implementations live in `unit/`, one module per area:
  - `unit::ledger`: the replay ledger and health events;
  - `unit::events`: recorded Slack names, ledger lookups, the GitHub read pause and
    the event feed's single reads;
  - `unit::views`: the read models behind the control API and the dashboard, and
    the owner's notes;
  - `unit::ingest`: catch-up watermarks, the Socket Mode status, attachment lookups
    and the worker supervisor's writes;
  - `unit::attention`: messages arriving in threads, reply capacity and obligations;
  - `unit::controls`: thread controls, the worker stops they queue, and linked
    threads;
  - `unit::neighbours`: the runtime row, thread memory, worker-result snapshots,
    reply evidence, debriefs, progress notes and the scheduling sweep;
  - `unit::actor`: loading, fencing, settling and committing a thread's turns;
  - `unit::modules`: the outbox, jobs and workers, approvals, scoped fetches, worker
    controls, diagnostics, links, the weekly archives and the configuration journal,
    over the modules below.
- `schema` and `migrations/*.sql`: the schema versions (`schema::VERSION`), applied in
  order, and the guards that count every durable write so an automatic rollback is
  refused once the daemon has written.
- `migration`: upgrading a stopped daemon's database with a backup and a crash-safe
  journal, resumable from any interruption and reversible until the daemon writes. A
  file the host migrates with the database (Fridica's `config.toml`) joins through the
  `Companion` trait: the store backs it up, fingerprints it and restores it; what it
  means is the host's.
- `archive`: weekly archive files beside the database. Quiet, finished threads move
  there with every row they own, completed replay events after a day; a thread comes
  back (with new row numbers) when a message arrives in it; `search` reads archives.
- `record`: what the replay ledger keeps of its bulkiest records (Slack responses as
  summaries, backend traffic as text).
- `links`: the channel ledger of which threads mention which pull requests, issues
  and other threads.
- `outbox`, `work`, `approvals`, `fetch`, `worker_controls`, `diagnostics`,
  `configuration`: the stores for posts, workers and jobs, approvals, scoped fetches,
  worker controls, diagnostics and configuration-edit intents.

`rusqlite` is re-exported for hosts whose own queries have not moved behind the traits
yet.

## Features

- `testing`: the migration engine's fault-injection entry points
  (`migrate_with_checkpoint`, `rollback_with_checkpoint`), for hosts' tests.

## License

MIT
