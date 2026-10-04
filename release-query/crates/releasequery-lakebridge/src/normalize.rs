//! Validate all raw keys before filtering tombstones or the common scope.
use crate::protocol::Scope;
use arrow::{array::*, compute::filter_record_batch, datatypes::*, record_batch::RecordBatch};
use datafusion::{
    common::Result,
    execution::{TaskContext, memory_pool::MemoryConsumer},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use releasequery_core::{
    exec::{Limits, error},
    schema::{detail_schema, live_schema},
};
use std::{any::Any, fmt, sync::Arc};
#[derive(Debug)]
pub struct NormalizeExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    scope: Scope,
    limits: Limits,
}
impl NormalizeExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, scope: Scope, limits: Limits) -> Result<Self> {
        scope.micros()?;
        limits.validate()?;
        let schema = input.schema();
        if schema.fields().len() != 8 {
            return Err(error("INCOMPATIBLE_SCHEMA: detail column count"));
        }
        for (i, (a, b)) in schema
            .fields()
            .iter()
            .zip(detail_schema().fields())
            .enumerate()
        {
            if a.name() != b.name()
                || !(a.data_type() == b.data_type()
                    || i == 2 && a.data_type() == &DataType::Timestamp(TimeUnit::Microsecond, None))
            {
                return Err(error("INCOMPATIBLE_SCHEMA: raw normalized input types"));
            }
        }
        let mut cache = input.properties().as_ref().clone();
        cache.eq_properties = EquivalenceProperties::new(live_schema());
        // Reconstruct to invalidate cached ordering expressions against the raw schema.
        let cache = PlanProperties::new(
            cache.eq_properties,
            cache.partitioning,
            cache.emission_type,
            cache.boundedness,
        );
        Ok(Self {
            input,
            cache: Arc::new(cache),
            scope,
            limits,
        })
    }
}
impl DisplayAs for NormalizeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NormalizeExec: validate all raw keys, live rows, tenant={}, UTC [{},{})",
            self.scope.tenant_id, self.scope.start, self.scope.end
        )
    }
}
impl ExecutionPlan for NormalizeExec {
    fn name(&self) -> &str {
        "NormalizeExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(error("NormalizeExec needs one child"));
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.scope.clone(),
            self.limits.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 || self.input.output_partitioning().partition_count() != 1 {
            return Err(error(
                "INVALID_REQUEST: normalization requires one ordered partition",
            ));
        }
        let stream = self.input.execute(0, context.clone())?;
        let limits = self.limits.clone();
        let scope = self.scope.clone();
        let (start, end) = scope.micros()?;
        let reservation =
            MemoryConsumer::new("NormalizeExec.previous_key").register(context.memory_pool());
        let stream = futures::stream::try_unfold(
            (stream, None::<(String, String)>, reservation),
            move |(mut input, mut previous, reservation)| {
                let scope = scope.clone();
                let limits = limits.clone();
                async move {
                    loop {
                        let Some(batch) = input.next().await.transpose()? else {
                            return Ok(None);
                        };
                        if batch.num_rows() > limits.input_batch_rows {
                            return Err(error("RESOURCE_EXHAUSTED: raw input batch row limit"));
                        }
                        for (i, a) in batch.columns().iter().enumerate() {
                            if [0, 1, 2, 7].contains(&i) && a.null_count() != 0 {
                                return Err(error("INVALID_KEYS: null raw key/time/tombstone"));
                            }
                            if let Some(s) = a.as_any().downcast_ref::<StringArray>() {
                                for v in s.iter().flatten() {
                                    if v.len() > limits.max_value_bytes {
                                        return Err(error("RESOURCE_EXHAUSTED: raw string limit"));
                                    }
                                }
                            }
                        }
                        if batch.num_rows() == 0 {
                            tokio::task::yield_now().await;
                            continue;
                        }
                        let tenants = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let events = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let times = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        let deleted = batch
                            .column(7)
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap();
                        let mut prev = previous.as_ref().map(|(a, b)| (a.as_str(), b.as_str()));
                        let mut mask = Vec::with_capacity(batch.num_rows());
                        for i in 0..batch.num_rows() {
                            let key = (tenants.value(i), events.value(i));
                            if key.0.is_empty()
                                || key.1.is_empty()
                                || prev.is_some_and(|p| p >= key)
                            {
                                return Err(error(
                                    "INVALID_KEYS: raw duplicate/empty/descending key, including tombstones and other tenants",
                                ));
                            }
                            prev = Some(key);
                            mask.push(
                                !deleted.value(i)
                                    && key.0 == scope.tenant_id
                                    && times.value(i) >= start
                                    && times.value(i) < end,
                            );
                        }
                        let last = prev.unwrap();
                        let old = reservation.size();
                        reservation.try_grow(last.0.len() + last.1.len() + 64)?;
                        previous = Some((last.0.into(), last.1.into()));
                        reservation.shrink(old);
                        let columns = batch
                            .columns()
                            .iter()
                            .take(7)
                            .enumerate()
                            .map(|(i, a)| {
                                if i == 2 {
                                    Arc::new(times.clone().with_timezone("UTC")) as ArrayRef
                                } else {
                                    a.clone()
                                }
                            })
                            .collect();
                        // Attaching UTC preserves numeric microseconds: LakeBridge's declared Spark-UTC
                        // adapter; no local-time inference or implicit decimal conversion.
                        let normalized = RecordBatch::try_new(live_schema(), columns)?;
                        let filtered = filter_record_batch(&normalized, &BooleanArray::from(mask))?;
                        if filtered.num_rows() != 0 {
                            return Ok(Some((filtered, (input, previous, reservation))));
                        }
                        tokio::task::yield_now().await;
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            live_schema(),
            stream,
        )))
    }
}
