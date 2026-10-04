# Execution contract

The generic core accepts non-null UTF8 `tenant_id,event_id` keys followed by
Int64, UTF8, Decimal128 or microsecond Timestamp payload fields. Both schemas must
match exactly, including decimal scale and timezone. Null equals null; null differs
from a value. Non-null values are compared directly in typed Arrow arrays, without
hash-only equality or JSON/Python conversion. Floats, dictionaries, nested types
and arbitrary user expressions are rejected. Nullable before/after structs carry
full live records; absent before means INSERT and absent after means DELETE.
Their physical children are nullable to match SQL outer-join Arrow output.

The LakeBridge adapter projects the fixed eight detail columns, globally sorts
all raw keys, validates uniqueness/non-null required fields, then removes
`is_deleted=true` and applies the common tenant and UTC `[start,end)` scope.
Raw key checks include excluded tenants and tombstones, matching the SQL backend.
It attaches UTC to Spark's declared UTC microsecond values without shifting them.
It does not cast decimals or infer local time. Compare columns are never pruned
because a preview hides them; no LIMIT or payload predicate is pushed into input.

`ReleaseDiffExec` declares two SinglePartition distribution and key-ordering
requirements. The adapter coalesces multiple scan partitions and wraps them in a
real global `SortExec`. DataFusion handles external sort/spill. An explicit
`inputs_sorted=true` assertion uses one scan partition and skips sorting; both the raw adapter and core
check order across batches and files; a false assertion fails. Per-file order is
not treated as global order. In particular, parallel Parquet byte-range scans may reorder an otherwise sorted file; the sorted fast path disables that repartitioning. Native input is sorted by default.

The pull-based merge retains one current batch/cursor per side, the previous key,
and a bounded typed output buffer. Empty batches and Pending are distinct from
EOF. A duplicate or descending key in a later batch fails the entire artifact,
even if earlier batches were written. A main-key change is DELETE plus INSERT.
No output appears for a file rewrite with identical business values.

The core creates no background tasks or prefetch queues. Dropping its stream
releases both child streams and memory reservations; a failed side prevents
further polling. It yields cooperatively every 4096 comparisons and on empty
batches. The CLI handles Ctrl-C by dropping the execution future. Failed or
cancelled private staging directories contain diagnostics without a complete
manifest; successful publication uses Linux `renameat2(RENAME_NOREPLACE)` and
fsync. A pre-existing output cannot be replaced. Rust does not implement its own
spill or write any business database state.

## Memory ownership

Defaults: at most 8192 rows per input batch, 4096 output rows, 1 MiB per UTF8
value, an 8 MiB output buffer, and a 512 MiB shared DataFusion pool. Options are
bounded; a single row too large for the output budget fails explicitly.

The core registers retained Arrow batches, previous-key copies and typed output
rows through DataFusion MemoryReservation. Batch buffer sizes are conservatively
counted per retained batch, so shared buffers can be counted on both sides and
again by upstream. Output reservations include spare Vec capacity, temporary
ScalarValue clones and Arrow construction. Actual field comparison uses the
arrays directly; typed owned values are copied only for changed output rows.
Output ownership transfers downstream at yield; the core releases its own buffer
reservation. The normalizer separately reserves its retained previous key.

When another output row cannot fit, accumulated output is yielded first and the
same input row is retried. Output is also flushed before crossing an input batch
boundary, so the operator never parks an unreserved newly fetched batch while
trying to free an old output buffer. If a retained batch or a single row still
cannot fit, execution returns a resource error. Tests check pool usage reaches
zero on EOF, input error, memory failure and drop, including a pressure case that
flushes and continues.

This accounting is **not a process RSS ceiling**. Scanners may allocate before a
batch can be inspected; Arrow filtering, Parquet output/compression, allocator
capacity and upstream library caches are not all registered with the pool. String
limits are checked after decoding. Native metadata planning is limited to 200,000
tasks and the descriptor JSON to 16 MiB. Peak core reservation, sort spill metrics
and externally sampled process-tree RSS are reported separately.

## Timing interpretation

Artifacts retain descriptor/plan preparation, inclusive scan pull wall time,
SortExec elapsed-compute/spill metrics, core input-pull time, typed merge CPU,
output construction and Parquet output write time. Inclusive scan pull covers the
scan and fixed projection; native provider preflight belongs to preparation.
Nested timings overlap and must not be summed into an invented total. Manifest
`total_ms` is the elapsed time before final manifest publication; the benchmark
measures the complete backend call and process wall time externally.

The native reader's internal caches/tasks are owned by Iceberg/DataFusion. Core
unit tests prove its own child-drop contract; the real native demo proves pinned
scan/parity, not every upstream reader's cancellation or deletion mode.
