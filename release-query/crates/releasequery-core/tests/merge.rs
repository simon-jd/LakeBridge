use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result},
    execution::{
        TaskContext,
        memory_pool::{GreedyMemoryPool, MemoryPool},
        runtime_env::RuntimeEnvBuilder,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, TryStreamExt};
use releasequery_core::exec::{Limits, ReleaseDiffExec};
use std::{
    any::Any,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
        Field::new("decimal", DataType::Decimal128(18, 6), true),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}
fn batch(keys: &[&str], values: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec!["tenant"; keys.len()])),
            Arc::new(StringArray::from(keys.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(
                Decimal128Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i128 * 1_000_000))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(18, 6)
                .unwrap(),
            ),
            Arc::new(TimestampMicrosecondArray::from(values.to_vec()).with_timezone("UTC")),
        ],
    )
    .unwrap()
}
#[derive(Debug)]
struct Source {
    batches: Vec<RecordBatch>,
    cache: Arc<PlanProperties>,
    fail: bool,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Source {
    fn new(batches: Vec<RecordBatch>, fail: bool) -> Arc<Self> {
        Self::with_schema(batches, fail, schema())
    }
    fn with_schema(batches: Vec<RecordBatch>, fail: bool, schema: SchemaRef) -> Arc<Self> {
        Arc::new(Self {
            batches,
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            fail,
            polls: Arc::default(),
            drops: Arc::default(),
        })
    }
}
struct DropGuard(Arc<AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl DisplayAs for Source {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestSource")
    }
}
impl ExecutionPlan for Source {
    fn name(&self) -> &str {
        "TestSource"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let items = self
            .batches
            .clone()
            .into_iter()
            .map(Ok)
            .chain(if self.fail {
                Some(Err(DataFusionError::Execution(
                    "injected upstream failure".into(),
                )))
            } else {
                None
            })
            .collect::<Vec<_>>();
        let polls = self.polls.clone();
        let guard = DropGuard(self.drops.clone());
        let stream = futures::stream::unfold((items.into_iter(), guard), move |(mut it, g)| {
            let polls = polls.clone();
            async move {
                tokio::task::yield_now().await;
                polls.fetch_add(1, Ordering::SeqCst);
                it.next().map(|x| (x, (it, g)))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
fn context(bytes: usize) -> (Arc<TaskContext>, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build()
        .unwrap();
    (
        Arc::new(TaskContext::default().with_runtime(Arc::new(runtime))),
        pool,
    )
}
#[tokio::test]
async fn typed_merge_cross_batch_pending_null_and_all_eof_shapes() {
    let cases = vec![
        (vec![], vec![], 0),
        (vec![], vec![batch(&["a"], &[None])], 1),
        (vec![batch(&["a"], &[None])], vec![], 1),
        (
            vec![
                batch(&[], &[]),
                batch(&["a", "b"], &[Some(1), None]),
                batch(&["d"], &[Some(3)]),
            ],
            vec![
                batch(&["a"], &[Some(2)]),
                batch(&[], &[]),
                batch(&["c", "d"], &[Some(4), Some(3)]),
            ],
            3,
        ),
    ];
    for (l, r, n) in cases {
        let plan = ReleaseDiffExec::try_new(
            Source::new(l, false),
            Source::new(r, false),
            Limits {
                output_batch_rows: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let (ctx, pool) = context(1 << 20);
        let batches = plan
            .execute(0, ctx)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), n);
        assert_eq!(pool.reserved(), 0);
        if n == 3 {
            let kinds = batches
                .iter()
                .map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0)
                })
                .collect::<Vec<_>>();
            assert_eq!(kinds, vec!["UPDATE", "DELETE", "INSERT"]);
        }
    }
}
#[tokio::test]
async fn duplicate_reverse_empty_key_and_upstream_failure_release_memory() {
    for (batches, fail) in [
        (vec![batch(&["a"], &[None]), batch(&["a"], &[None])], false),
        (vec![batch(&["b", "a"], &[None, None])], false),
        (vec![batch(&[""], &[None])], false),
        (vec![batch(&["a"], &[None])], true),
    ] {
        let l = Source::new(batches, fail);
        let r = Source::new(vec![], false);
        let plan = ReleaseDiffExec::try_new(
            l.clone(),
            r.clone(),
            Limits {
                output_batch_rows: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let (ctx, pool) = context(1 << 20);
        let result = plan.execute(0, ctx).unwrap().try_collect::<Vec<_>>().await;
        assert!(result.is_err());
        assert_eq!(pool.reserved(), 0);
        assert_eq!(l.drops.load(Ordering::SeqCst), 1);
        assert_eq!(r.drops.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn drop_cancels_without_more_pulls_and_releases_reservations() {
    let l = Source::new(
        vec![batch(&["a", "b"], &[Some(1), None]), batch(&["c"], &[None])],
        false,
    );
    let r = Source::new(vec![], false);
    let plan = ReleaseDiffExec::try_new(
        l.clone(),
        r.clone(),
        Limits {
            output_batch_rows: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let (ctx, pool) = context(1 << 20);
    let mut stream = plan.execute(0, ctx).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    assert!(pool.reserved() > 0);
    let polls = l.polls.load(Ordering::SeqCst);
    drop(stream);
    tokio::task::yield_now().await;
    assert_eq!(pool.reserved(), 0);
    assert_eq!(l.polls.load(Ordering::SeqCst), polls);
    assert_eq!(l.drops.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn memory_and_value_limits_fail_cleanly() {
    for (bytes, limits) in [
        (1, Limits::default()),
        (
            1 << 20,
            Limits {
                max_value_bytes: 2,
                ..Default::default()
            },
        ),
        (
            1 << 20,
            Limits {
                input_batch_rows: 1,
                ..Default::default()
            },
        ),
        (
            1 << 20,
            Limits {
                output_buffer_bytes: 1024,
                ..Default::default()
            },
        ),
    ] {
        let plan = ReleaseDiffExec::try_new(
            Source::new(vec![batch(&["a", "b"], &[None, None])], false),
            Source::new(vec![], false),
            limits,
        )
        .unwrap();
        let (ctx, pool) = context(bytes);
        assert!(
            plan.execute(0, ctx)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert_eq!(pool.reserved(), 0);
    }
}
#[test]
fn schemas_and_partition_contract() {
    let input = Source::new(vec![], false);
    let plan = ReleaseDiffExec::try_new(input.clone(), input, Limits::default()).unwrap();
    assert_eq!(plan.required_input_distribution().len(), 2);
    assert!(plan.required_input_ordering().iter().all(Option::is_some));
    assert!(plan.execute(1, context(1 << 20).0).is_err());
    for datatype in [
        DataType::Float64,
        DataType::Boolean,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    ] {
        let s = Schema::new(vec![
            schema().field(0).clone(),
            schema().field(1).clone(),
            Field::new("bad", datatype, true),
        ]);
        assert!(releasequery_core::exec::validate_schema(&s).is_err());
    }
}

#[tokio::test]
async fn memory_pressure_flushes_pending_rows_then_continues() {
    let rows = batch(&["a", "b", "c"], &[None, None, None]);
    let budget = rows.get_array_memory_size() + 64 + "tenant".len() + 1 + 3100;
    let plan = ReleaseDiffExec::try_new(
        Source::new(vec![rows], false),
        Source::new(vec![], false),
        Limits::default(),
    )
    .unwrap();
    let (ctx, pool) = context(budget);
    let batches = plan
        .execute(0, ctx)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    assert!(batches.len() > 1);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn decimal_and_timestamp_are_compared_even_when_other_payload_is_equal() {
    let left = batch(&["a", "b"], &[Some(1), Some(1)]);
    let mut columns = left.columns().to_vec();
    columns[3] = Arc::new(
        Decimal128Array::from(vec![1_000_001, 1_000_000])
            .with_precision_and_scale(18, 6)
            .unwrap(),
    );
    columns[4] = Arc::new(TimestampMicrosecondArray::from(vec![1, 2]).with_timezone("UTC"));
    let right = RecordBatch::try_new(schema(), columns).unwrap();
    let plan = ReleaseDiffExec::try_new(
        Source::new(vec![left], false),
        Source::new(vec![right], false),
        Limits::default(),
    )
    .unwrap();
    let batches = plan
        .execute(0, context(1 << 20).0)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    for b in batches {
        assert!(
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .all(|x| x == Some("UPDATE"))
        );
    }
}

fn numeric_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("flag_a", DataType::Int32, true),
        Field::new("flag_b", DataType::Int32, true),
    ]))
}
fn numeric_batch(keys: Vec<i64>, values: Vec<Option<f64>>) -> RecordBatch {
    let n = keys.len();
    RecordBatch::try_new(
        numeric_schema(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(values)),
            Arc::new(TimestampNanosecondArray::from(vec![Some(100); n])),
            Arc::new(Int32Array::from(vec![Some(1); n])),
            Arc::new(Int32Array::from(vec![None::<i32>; n])),
        ],
    )
    .unwrap()
}
fn numeric_plan(
    left: Vec<RecordBatch>,
    right: Vec<RecordBatch>,
    policy: releasequery_core::numeric::FloatComparison,
) -> ReleaseDiffExec {
    ReleaseDiffExec::try_new_numeric(
        Source::with_schema(left, false, numeric_schema()),
        Source::with_schema(right, false, numeric_schema()),
        Limits {
            input_batch_rows: 65536,
            output_batch_rows: 65536,
            ..Default::default()
        },
        policy,
    )
    .unwrap()
}
#[test]
fn numeric_tolerance_nan_infinity_zero_and_nontransitivity() {
    use releasequery_core::numeric::{FloatComparison, NanPolicy};
    let p = FloatComparison::default();
    for (a, b, equal) in [
        (0.0, -0.0, true),
        (f64::INFINITY, f64::INFINITY, true),
        (f64::NEG_INFINITY, f64::INFINITY, false),
        (f64::INFINITY, f64::MAX, false),
        (f64::NAN, f64::NAN, true),
        (f64::NAN, 1.0, false),
        (1.0, 1.0 + 1e-7, true),
        (1.0, 1.0 + 1e-4, false),
        (f64::MAX, -f64::MAX, false),
    ] {
        assert_eq!(p.equivalent(a, b), equal);
    }
    assert!(
        !FloatComparison {
            nan_policy: NanPolicy::Unequal,
            ..p
        }
        .equivalent(f64::NAN, f64::NAN)
    );
    let fixed = FloatComparison {
        absolute: 1.0,
        relative: 0.0,
        ..p
    };
    assert!(fixed.equivalent(0.0, 1.0));
    assert!(fixed.equivalent(1.0, 2.0));
    assert!(!fixed.equivalent(0.0, 2.0));
    for absolute in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(FloatComparison { absolute, ..p }.validate().is_err());
    }
}
#[tokio::test]
async fn numeric_merge_uses_exact_keys_nullable_payloads_and_each_column() {
    let left = numeric_batch(
        vec![i64::MIN, -1, 0, 1, 2, 3, i64::MAX],
        vec![
            None,
            Some(f64::NAN),
            Some(-0.0),
            Some(f64::INFINITY),
            Some(10.0),
            Some(1.0),
            Some(4.0),
        ],
    );
    let right = numeric_batch(
        vec![i64::MIN, -1, 0, 1, 2, 4, i64::MAX],
        vec![
            Some(0.0),
            Some(f64::NAN),
            Some(0.0),
            Some(f64::INFINITY),
            Some(10.0 + 1e-7),
            Some(1.0),
            Some(4.0),
        ],
    );
    let mut columns = right.columns().to_vec();
    columns[3] = Arc::new(Int32Array::from(vec![1, 1, 1, 1, 1, 1, 2]));
    let right = RecordBatch::try_new(numeric_schema(), columns).unwrap();
    let plan = numeric_plan(vec![left], vec![right], Default::default());
    let (ctx, pool) = context(1 << 20);
    let batches = plan
        .execute(0, ctx)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let keys = batch
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| (kinds.value(i).to_owned(), keys.value(i)))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("UPDATE".into(), i64::MIN),
            ("DELETE".into(), 3),
            ("INSERT".into(), 4),
            ("UPDATE".into(), i64::MAX)
        ]
    );
    assert_eq!(pool.reserved(), 0);
}
#[tokio::test]
async fn numeric_duplicate_descending_keys_and_schema_drift_fail_closed() {
    for rows in [
        vec![
            numeric_batch(vec![1], vec![None]),
            numeric_batch(vec![1], vec![None]),
        ],
        vec![numeric_batch(vec![2, 1], vec![None, None])],
        vec![batch(&["a"], &[None])],
    ] {
        let plan = numeric_plan(rows, vec![], Default::default());
        let (ctx, pool) = context(1 << 20);
        assert!(
            plan.execute(0, ctx)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert_eq!(pool.reserved(), 0);
    }
}
#[tokio::test]
async fn numeric_64k_batches_remain_bounded_and_errors_release_memory() {
    let rows = numeric_batch((0..65536).collect(), vec![Some(1.0); 65536]);
    let plan = numeric_plan(vec![rows.clone()], vec![rows], Default::default());
    let stats = plan.stats();
    let (ctx, pool) = context(16 << 20);
    assert!(
        plan.execute(0, ctx)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(pool.reserved(), 0);
    let stats = stats.lock().unwrap();
    assert_eq!((stats.left_rows, stats.right_rows), (65536, 65536));
    assert!(stats.peak_reserved_bytes < 8 << 20);
}
