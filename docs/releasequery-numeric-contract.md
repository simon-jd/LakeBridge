# Ordered numeric snapshot comparison

ReleaseQuery exposes two explicit profiles. The existing `diff REQUEST --output
DIRECTORY` protocol compares fixed LakeBridge releases with exact business fields
and remains compatible with `RustDiffBackend`. The separate `numeric-diff`
command implements the five-column projection in the experience bank. It accepts
two already ordered local Parquet files and writes one new Parquet change file.
It does not infer release pins or change consumer pointers.

## Reproduce the numeric profile

With Rust 1.94.0 and the repository's Python consumer dependencies installed:

```bash
cargo build --manifest-path release-query/Cargo.toml --locked --release
release-query/target/release/release-query --threads 4 numeric-diff \
  --left left.parquet --right right.parquet --output changes.parquet \
  --columns key,value,timestamp,flag_a,flag_b \
  --batch-rows 65536 --memory-mib 1024 --nan-policy equal
.venv/bin/python release-query/tests/numeric_oracle.py \
  --binary release-query/target/release/release-query \
  --output artifacts/numeric-oracle.json
```

The five selected fields, in order, must be a non-nullable `int64` key, `float64`
value, Arrow timestamp with a 64-bit underlying value, and two `int32` fields.
Field names may be supplied using `--columns`; defaults are shown above. Both
inputs must have identical projected schemas, including timestamp unit and
timezone. Payload fields may be nullable. The key is exact over the full signed
64-bit range and must be strictly ascending. Duplicate or descending keys within
or across RecordBatches fail the operation. There is no implicit sort.

Each output row contains `change_type`, a key struct, and before/after row structs.
Keys present on one side yield INSERT or DELETE. A shared key yields UPDATE if
any payload column differs. Every payload column is compared directly; hashes
never decide equality.

## Numeric rules

For finite values the default predicate is:

```text
abs(left - right) <= 1e-8 + 1e-6 * max(abs(left), abs(right))
```

`--absolute-tolerance` and `--relative-tolerance` override these values; both must
be finite, absolute tolerance must be nonnegative, and relative tolerance must be
between zero and one. When subtraction or the threshold overflows, normalized
arithmetic prevents a spurious `infinity <= infinity` match. Tolerance equality
is intentionally not transitive and is never used to order keys or form groups.

NULL matches only NULL, independently in each column. `--nan-policy equal`
(default) makes NaN match NaN; `unequal` emits an update for two NaNs. Neither
policy equates a NaN with a finite value, infinity or NULL. Same-sign infinities
and either sign of zero match. Integer and timestamp payloads compare exactly;
timestamps are not converted through Python datetimes or floating-point seconds.

## Streaming, scheduling and publication

The merge is pull based. It retains one current input batch per side, previous
boundary keys, and a bounded pending output batch. It introduces no background
producer queue. Input and output batches default to 65,536 rows and cannot exceed
that size. The sink retains one returned output batch and bounds Parquet row
groups by the selected batch size. Full tables and full change sets are never
collected by the Rust command.

`--threads T` is a shared Tokio execution budget, accepted from 2 through 64.
A current-thread runtime executes asynchronous work on the main thread, with at
most T−1 blocking workers; it does not add a separate async worker beside the
main thread. DataFusion uses that runtime, with one target partition for this
ordered profile and no outer parallel pool. This bounds these executors rather
than claiming a bound on threads external libraries or processes might create.
The JSON summary records both thread components.

The DataFusion pool and merge reservations enforce `--memory-mib`; this is not a
process RSS cap. Allocator overhead, Parquet writer buffers and OS file cache are
outside that accounting. The regression verifies bounded merge reservations for
64K-row input batches and releases reservations after errors. Use an external RSS
sampler for a performance qualification.

Output is staged beside its destination. Only a successful complete scan and
closed writer are fsynced and published without replacing an existing file;
duplicate keys, schema errors and other scan failures leave no final artifact.
The JSON reports input rows/s using both input row counts, not diff rows. It
separates scan/merge/bounded-sink time from the final fsync/publication phase.
Parquet encoding is part of this command's bounded sink; these local timings
must not be substituted for a different benchmark's core-only definition.

## Verification and result provenance

The independent [Python oracle](../release-query/tests/numeric_oracle.py) uses a
set-based reference comparison and PyArrow only for file interchange. The local
run covered 20 seeded random datasets under both NaN policies, tolerance
neighbors, output no-overwrite, cross-batch duplicates and descending keys:
43 cases passed. Rust regressions separately cover full-range int64 keys,
NULLs, every payload type, schema drift, memory release and 64K batches.

See [current validation](github-alignment-validation.md) and the checked-in
[oracle output](evidence/github-alignment-20261004/numeric-oracle.json). The
100M-per-input, 22-second/1.1-GiB and end-to-end 52-to-14-minute measurements in
the README remain the owner's independently executed cloud qualification. This
small local correctness run did not repeat those throughput or RSS experiments.
