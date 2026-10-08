# Vendored matching engine

These four headers are copied from the `matching-engine` project (MIT, same
author). Two additions support snapshots; nothing else changed:

- `Engine::restore` in `engine.hpp` rebuilds a book from saved orders while
  preserving every FIFO priority.
- `Digest(std::uint64_t)` in `types.hpp` resumes an event fingerprint from a
  saved value.

QuorumLog treats the engine as an opaque deterministic state machine: the same
command log must produce the same event fingerprint and state digest on every
replica and in the standalone `matching-engine` binary.
