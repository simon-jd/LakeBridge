//! Exact, pull-based merge of two globally ordered, bounded Arrow streams.
use crate::numeric::FloatComparison;
use arrow::{array::*, buffer::NullBuffer, datatypes::*, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    execution::{
        TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    physical_expr::{
        EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalSortExpr,
        expressions::Column,
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PlanProperties, SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    cmp::Ordering,
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

pub fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub input_batch_rows: usize,
    pub output_batch_rows: usize,
    pub max_value_bytes: usize,
    pub output_buffer_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            input_batch_rows: 8192,
            output_batch_rows: 4096,
            max_value_bytes: 1 << 20,
            output_buffer_bytes: 8 << 20,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        if !(1..=65536).contains(&self.input_batch_rows)
            || !(1..=65536).contains(&self.output_batch_rows)
            || !(1..=16 * 1024 * 1024).contains(&self.max_value_bytes)
            || !(1024..=256 * 1024 * 1024).contains(&self.output_buffer_bytes)
        {
            return Err(error("INVALID_REQUEST: invalid resource limits"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct Stats {
    pub left_rows: u64,
    pub right_rows: u64,
    pub output_rows: u64,
    pub input_pull_ns: u64,
    pub merge_cpu_ns: u64,
    pub output_build_ns: u64,
    pub peak_reserved_bytes: usize,
}
pub type SharedStats = Arc<Mutex<Stats>>;

pub fn key_order() -> LexOrdering {
    LexOrdering::new(
        vec!["tenant_id", "event_id"]
            .into_iter()
            .enumerate()
            .map(|(i, n)| PhysicalSortExpr {
                expr: Arc::new(Column::new(n, i)),
                options: arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            })
            .collect::<Vec<_>>(),
    )
    .expect("two keys")
}
pub fn validate_schema(schema: &Schema) -> Result<()> {
    validate_unique_names(schema)?;
    if schema.fields().len() < 2
        || schema.fields().iter().take(2).enumerate().any(|(i, f)| {
            f.name() != ["tenant_id", "event_id"][i]
                || f.data_type() != &DataType::Utf8
                || f.is_nullable()
        })
    {
        return Err(error(
            "INCOMPATIBLE_SCHEMA: first two fields must be non-null UTF8 keys",
        ));
    }
    for f in schema.fields() {
        match f.data_type() {
            DataType::Utf8
            | DataType::Int64
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(TimeUnit::Microsecond, _) => (),
            _ => {
                return Err(error(format!(
                    "INCOMPATIBLE_SCHEMA: unsupported type {}",
                    f.data_type()
                )));
            }
        }
    }
    Ok(())
}
fn validate_unique_names(schema: &Schema) -> Result<()> {
    let names: std::collections::HashSet<_> =
        schema.fields().iter().map(|field| field.name()).collect();
    if names.len() != schema.fields().len() {
        return Err(error("INCOMPATIBLE_SCHEMA: duplicate column names"));
    }
    Ok(())
}

pub fn validate_numeric_schema(schema: &Schema) -> Result<()> {
    validate_unique_names(schema)?;
    let valid = schema.fields().len() == 5
        && schema.field(0).data_type() == &DataType::Int64
        && !schema.field(0).is_nullable()
        && schema.field(1).data_type() == &DataType::Float64
        && matches!(schema.field(2).data_type(), DataType::Timestamp(_, _))
        && schema.field(3).data_type() == &DataType::Int32
        && schema.field(4).data_type() == &DataType::Int32;
    if !valid {
        return Err(error(
            "INCOMPATIBLE_SCHEMA: numeric profile requires non-null int64 key, float64, timestamp64, int32, int32",
        ));
    }
    Ok(())
}

pub fn output_schema(input: &Schema) -> SchemaRef {
    output_schema_for(input, 2)
}
fn output_schema_for(input: &Schema, keys: usize) -> SchemaRef {
    let fields: Fields = input
        .fields()
        .iter()
        .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), true)))
        .collect();
    Arc::new(Schema::new(vec![
        Field::new("change_type", DataType::Utf8, false),
        Field::new(
            "key",
            DataType::Struct(fields.iter().take(keys).cloned().collect()),
            false,
        ),
        Field::new("before", DataType::Struct(fields.clone()), true),
        Field::new("after", DataType::Struct(fields), true),
    ]))
}
#[derive(Debug)]
pub struct ReleaseDiffExec {
    children: [Arc<dyn ExecutionPlan>; 2],
    cache: Arc<PlanProperties>,
    limits: Limits,
    stats: SharedStats,
    numeric: Option<FloatComparison>,
}
impl ReleaseDiffExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        limits: Limits,
    ) -> Result<Self> {
        Self::create(left, right, limits, None)
    }
    /// Five projected columns, exact int64 primary key and explicit float policy.
    pub fn try_new_numeric(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        limits: Limits,
        comparison: FloatComparison,
    ) -> Result<Self> {
        comparison.validate()?;
        Self::create(left, right, limits, Some(comparison))
    }
    fn create(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        limits: Limits,
        numeric: Option<FloatComparison>,
    ) -> Result<Self> {
        limits.validate()?;
        if numeric.is_some() {
            validate_numeric_schema(left.schema().as_ref())?;
        } else {
            validate_schema(left.schema().as_ref())?;
        }
        if left.schema() != right.schema() {
            return Err(error(
                "INCOMPATIBLE_SCHEMA: schemas differ; explicit adapter required",
            ));
        }
        let schema = output_schema_for(
            left.schema().as_ref(),
            if numeric.is_some() { 1 } else { 2 },
        );
        Ok(Self {
            children: [left, right],
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            limits,
            stats: Arc::default(),
            numeric,
        })
    }
    pub fn stats(&self) -> SharedStats {
        self.stats.clone()
    }
}
impl DisplayAs for ReleaseDiffExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ReleaseDiffExec: profile={}, output_batch_rows={}",
            if self.numeric.is_some() {
                "numeric-tolerance-int64-key"
            } else {
                "exact-tenant-event"
            },
            self.limits.output_batch_rows
        )
    }
}
impl ExecutionPlan for ReleaseDiffExec {
    fn name(&self) -> &str {
        "ReleaseDiffExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; 2]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let order = if self.numeric.is_some() {
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: Arc::new(Column::new(self.children[0].schema().field(0).name(), 0)),
                options: arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            }])
            .unwrap()
        } else {
            key_order()
        };
        vec![Some(OrderingRequirements::from(order)); 2]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(error("ReleaseDiffExec requires two inputs"));
        }
        Ok(Arc::new(Self::create(
            children[0].clone(),
            children[1].clone(),
            self.limits.clone(),
            self.numeric,
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0
            || self
                .children
                .iter()
                .any(|p| p.output_partitioning().partition_count() != 1)
        {
            return Err(error(
                "INVALID_REQUEST: each input must be one globally ordered partition",
            ));
        }
        let left = Cursor::new(
            self.children[0].execute(0, context.clone())?,
            &context,
            "left",
            self.numeric.is_some(),
        );
        let right = Cursor::new(
            self.children[1].execute(0, context.clone())?,
            &context,
            "right",
            self.numeric.is_some(),
        );
        let state = Merge {
            left,
            right,
            schema: self.schema(),
            input_schema: self.children[0].schema(),
            limits: self.limits.clone(),
            pending: Vec::new(),
            reservation: MemoryConsumer::new("ReleaseDiffExec.output")
                .register(context.memory_pool()),
            stats: self.stats.clone(),
            steps: 0,
            numeric: self.numeric,
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            match state.next().await? {
                Some(batch) => Ok(Some((batch, state))),
                None => Ok(None),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Text(String, String),
    Integer(i64),
}
impl Key {
    fn bytes(&self) -> usize {
        match self {
            Self::Text(a, b) => a.len() + b.len() + 64,
            Self::Integer(_) => 64,
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum KeyRef<'a> {
    Text(&'a str, &'a str),
    Integer(i64),
}
impl KeyRef<'_> {
    fn owned(self) -> Key {
        match self {
            Self::Text(a, b) => Key::Text(a.to_owned(), b.to_owned()),
            Self::Integer(v) => Key::Integer(v),
        }
    }
}
impl Key {
    fn borrowed(&self) -> KeyRef<'_> {
        match self {
            Self::Text(a, b) => KeyRef::Text(a, b),
            Self::Integer(v) => KeyRef::Integer(*v),
        }
    }
}
fn batch_key(batch: &RecordBatch, row: usize, numeric: bool) -> KeyRef<'_> {
    if numeric {
        KeyRef::Integer(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )
    } else {
        KeyRef::Text(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row),
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row),
        )
    }
}
struct Cursor {
    stream: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    row: usize,
    eof: bool,
    previous: Option<Key>,
    reservation: MemoryReservation,
    numeric: bool,
    expected_schema: SchemaRef,
}
impl Cursor {
    fn new(
        stream: SendableRecordBatchStream,
        context: &TaskContext,
        name: &str,
        numeric: bool,
    ) -> Self {
        let expected_schema = stream.schema();
        Self {
            stream,
            batch: None,
            row: 0,
            eof: false,
            previous: None,
            numeric,
            expected_schema,
            reservation: MemoryConsumer::new(format!("ReleaseDiffExec.{name}"))
                .register(context.memory_pool()),
        }
    }
    fn boundary(&self) -> bool {
        self.batch.as_ref().is_none_or(|b| self.row == b.num_rows())
    }
    fn key(&self) -> KeyRef<'_> {
        let b = self.batch.as_ref().expect("loaded row");
        batch_key(b, self.row, self.numeric)
    }
    async fn load(&mut self, limits: &Limits) -> Result<u64> {
        if self.eof || !self.boundary() {
            return Ok(0);
        }
        self.batch = None;
        let prev_bytes = self.previous.as_ref().map_or(0, Key::bytes);
        self.reservation
            .shrink(self.reservation.size().saturating_sub(prev_bytes));
        loop {
            let Some(batch) = self.stream.next().await.transpose()? else {
                self.eof = true;
                return Ok(0);
            };
            if batch.schema() != self.expected_schema {
                return Err(error("INCOMPATIBLE_SCHEMA: input stream changed schema"));
            }
            if batch.num_rows() > limits.input_batch_rows {
                return Err(error("RESOURCE_EXHAUSTED: input batch row limit"));
            }
            if batch.num_rows() == 0 {
                tokio::task::yield_now().await;
                continue;
            }
            // Conservative accounting: shared Arrow buffers are counted once per retained
            // batch (possibly twice across inputs), in addition to upstream reservations.
            let bytes = batch.get_array_memory_size();
            self.reservation.try_grow(bytes)?;
            for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                if !field.is_nullable() && array.null_count() != 0 {
                    return Err(error("INVALID_KEYS: null required field"));
                }
                if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
                    for s in strings.iter().flatten() {
                        if s.len() > limits.max_value_bytes {
                            return Err(error("RESOURCE_EXHAUSTED: maximum string bytes"));
                        }
                    }
                }
            }
            let mut previous = self.previous.as_ref().map(Key::borrowed);
            for i in 0..batch.num_rows() {
                let key = batch_key(&batch, i, self.numeric);
                if matches!(key, KeyRef::Text(a, b) if a.is_empty() || b.is_empty())
                    || previous.is_some_and(|p| p >= key)
                {
                    return Err(error(
                        "INVALID_KEYS: empty, duplicate or descending key across input batches",
                    ));
                }
                previous = Some(key);
            }
            let last = previous.unwrap();
            let next_bytes = match last {
                KeyRef::Text(a, b) => a.len() + b.len() + 64,
                KeyRef::Integer(_) => 64,
            };
            // Reserve the new key before allocation, then release the old copy.
            self.reservation.try_grow(next_bytes)?;
            self.previous = Some(last.owned());
            self.reservation.shrink(prev_bytes);
            let rows = batch.num_rows() as u64;
            self.row = 0;
            self.batch = Some(batch);
            return Ok(rows);
        }
    }
    fn advance(&mut self) {
        self.row += 1;
    }
    fn row_bytes(&self) -> usize {
        let b = self.batch.as_ref().unwrap();
        b.columns()
            .iter()
            .map(|a| {
                if a.is_null(self.row) {
                    96
                } else if let Some(s) = a.as_any().downcast_ref::<StringArray>() {
                    96 + s.value(self.row).len()
                } else {
                    112
                }
            })
            .sum::<usize>()
            + 64
    }
    fn owned(&self) -> Result<Vec<ScalarValue>> {
        self.batch
            .as_ref()
            .unwrap()
            .columns()
            .iter()
            .map(|a| ScalarValue::try_from_array(a, self.row))
            .collect()
    }
}
fn same(left: &Cursor, right: &Cursor, numeric: Option<FloatComparison>) -> bool {
    let l = left.batch.as_ref().unwrap();
    let r = right.batch.as_ref().unwrap();
    l.columns()
        .iter()
        .zip(r.columns())
        .skip(if numeric.is_some() { 1 } else { 2 })
        .all(|(a, b)| {
            let an = a.is_null(left.row);
            let bn = b.is_null(right.row);
            if an || bn {
                return an == bn;
            }
            macro_rules! eq {
                ($t:ty) => {
                    a.as_any().downcast_ref::<$t>().unwrap().value(left.row)
                        == b.as_any().downcast_ref::<$t>().unwrap().value(right.row)
                };
            }
            match a.data_type() {
                DataType::Utf8 => eq!(StringArray),
                DataType::Int64 => eq!(Int64Array),
                DataType::Int32 => eq!(Int32Array),
                DataType::Float64 => numeric.unwrap().equivalent(
                    a.as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(left.row),
                    b.as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(right.row),
                ),
                DataType::Decimal128(_, _) => eq!(Decimal128Array),
                DataType::Timestamp(TimeUnit::Second, _) => eq!(TimestampSecondArray),
                DataType::Timestamp(TimeUnit::Millisecond, _) => eq!(TimestampMillisecondArray),
                DataType::Timestamp(TimeUnit::Microsecond, _) => eq!(TimestampMicrosecondArray),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => eq!(TimestampNanosecondArray),
                _ => unreachable!("validated types"),
            }
        })
}
struct Change {
    kind: &'static str,
    before: Option<Vec<ScalarValue>>,
    after: Option<Vec<ScalarValue>>,
}
struct Merge {
    left: Cursor,
    right: Cursor,
    schema: SchemaRef,
    input_schema: SchemaRef,
    limits: Limits,
    pending: Vec<Change>,
    reservation: MemoryReservation,
    stats: SharedStats,
    steps: usize,
    numeric: Option<FloatComparison>,
}
impl Merge {
    fn peak(&self) {
        let bytes =
            self.reservation.size() + self.left.reservation.size() + self.right.reservation.size();
        let mut s = self.stats.lock().unwrap();
        s.peak_reserved_bytes = s.peak_reserved_bytes.max(bytes);
    }
    fn flush(&mut self) -> Result<RecordBatch> {
        let start = Instant::now();
        let rows = self.pending.len();
        let children: Fields = self
            .input_schema
            .fields()
            .iter()
            .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), true)))
            .collect();
        let make = |before: bool| -> Result<ArrayRef> {
            let mut columns = Vec::with_capacity(children.len());
            for (i, f) in children.iter().enumerate() {
                let null = ScalarValue::try_from(f.data_type())?;
                columns.push(ScalarValue::iter_to_array(self.pending.iter().map(|c| {
                    let record = if before { &c.before } else { &c.after };
                    record
                        .as_ref()
                        .map_or_else(|| null.clone(), |v| v[i].clone())
                }))?);
            }
            let valid = NullBuffer::from(
                self.pending
                    .iter()
                    .map(|c| {
                        if before {
                            c.before.is_some()
                        } else {
                            c.after.is_some()
                        }
                    })
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(StructArray::new(
                children.clone(),
                columns,
                Some(valid),
            )))
        };
        let mut keys = Vec::new();
        let key_count = if self.numeric.is_some() { 1 } else { 2 };
        for i in 0..key_count {
            keys.push(ScalarValue::iter_to_array(self.pending.iter().map(|c| {
                c.before.as_ref().or(c.after.as_ref()).unwrap()[i].clone()
            }))?);
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    self.pending.iter().map(|c| c.kind).collect::<Vec<_>>(),
                )),
                Arc::new(StructArray::new(
                    children.iter().take(key_count).cloned().collect(),
                    keys,
                    None,
                )),
                make(true)?,
                make(false)?,
            ],
        )?;
        self.pending.clear();
        self.pending.shrink_to_fit();
        self.reservation.free();
        let mut stats = self.stats.lock().unwrap();
        stats.output_rows += rows as u64;
        stats.output_build_ns += start.elapsed().as_nanos() as u64;
        Ok(batch)
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            // Flush before pulling another batch: never retain an unreserved incoming
            // batch while yielding output to relieve memory pressure.
            if !self.pending.is_empty()
                && ((!self.left.eof && self.left.boundary())
                    || (!self.right.eof && self.right.boundary())
                    || self.pending.len() >= self.limits.output_batch_rows)
            {
                return self.flush().map(Some);
            }
            let start = Instant::now();
            let left = self.left.load(&self.limits).await?;
            let right = self.right.load(&self.limits).await?;
            {
                let mut stats = self.stats.lock().unwrap();
                stats.left_rows += left;
                stats.right_rows += right;
                stats.input_pull_ns += start.elapsed().as_nanos() as u64;
            }
            self.peak();
            if self.left.eof && self.right.eof {
                return if self.pending.is_empty() {
                    Ok(None)
                } else {
                    self.flush().map(Some)
                };
            }
            self.steps += 1;
            if self.steps.is_multiple_of(4096) {
                tokio::task::yield_now().await;
            }
            let start = Instant::now();
            let order = if self.left.eof {
                Ordering::Greater
            } else if self.right.eof {
                Ordering::Less
            } else {
                self.left.key().cmp(&self.right.key())
            };
            let changed = order != Ordering::Equal || !same(&self.left, &self.right, self.numeric);
            if changed {
                let before = order != Ordering::Greater;
                let after = order != Ordering::Less;
                // Includes typed owned rows, spare Vec capacity, temporary ScalarValue
                // clones and Arrow construction. Final output ownership transfers downstream.
                let bytes = 4
                    * (if before { self.left.row_bytes() } else { 0 }
                        + if after { self.right.row_bytes() } else { 0 })
                    + 256;
                if bytes > self.limits.output_buffer_bytes {
                    return Err(error(
                        "RESOURCE_EXHAUSTED: one output row exceeds buffer budget",
                    ));
                }
                if self.reservation.size() + bytes > self.limits.output_buffer_bytes
                    && !self.pending.is_empty()
                {
                    return self.flush().map(Some);
                }
                if let Err(e) = self.reservation.try_grow(bytes) {
                    if !self.pending.is_empty() {
                        return self.flush().map(Some);
                    }
                    return Err(e);
                }
                self.peak();
                self.pending.push(Change {
                    kind: match order {
                        Ordering::Less => "DELETE",
                        Ordering::Greater => "INSERT",
                        Ordering::Equal => "UPDATE",
                    },
                    before: if before {
                        Some(self.left.owned()?)
                    } else {
                        None
                    },
                    after: if after {
                        Some(self.right.owned()?)
                    } else {
                        None
                    },
                });
            }
            if order != Ordering::Greater {
                self.left.advance();
            }
            if order != Ordering::Less {
                self.right.advance();
            }
            self.stats.lock().unwrap().merge_cpu_ns += start.elapsed().as_nanos() as u64;
        }
    }
}
