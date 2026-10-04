//! Inclusive scan/projection pull wall time, independently from SortExec CPU time.
use datafusion::{
    common::Result,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use std::{any::Any, fmt, sync::Arc, time::Instant};
#[derive(Debug)]
pub struct ScanTimerExec {
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}
impl ScanTimerExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}
impl DisplayAs for ScanTimerExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PinnedScanTimerExec: inclusive pull wall time")
    }
}
impl ExecutionPlan for ScanTimerExec {
    fn name(&self) -> &str {
        "PinnedScanTimerExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(releasequery_core::exec::error(
                "Scan timer requires one child",
            ));
        }
        Ok(Arc::new(Self::new(children[0].clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let timer = MetricBuilder::new(&self.metrics).subset_time("scan_pull_wall", partition);
        let stream = futures::stream::unfold(stream, move |mut input| {
            let timer = timer.clone();
            async move {
                let start = Instant::now();
                let item = input.next().await;
                timer.add_duration(start.elapsed());
                item.map(|item| (item, input))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
