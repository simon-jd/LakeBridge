//! Bounded local sink for the ordered five-column numeric profile.
use arrow::array::StringArray;
use clap::Args as ClapArgs;
use datafusion::{
    execution::{memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder},
    physical_plan::ExecutionPlan,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use futures::TryStreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use releasequery_core::{
    exec::{Limits, ReleaseDiffExec},
    numeric::{FloatComparison, NanPolicy},
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Instant};

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long)]
    left: PathBuf,
    #[arg(long)]
    right: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// In order: exact int64 key, float64, timestamp64, int32, int32.
    #[arg(
        long,
        value_delimiter = ',',
        num_args = 5,
        default_value = "key,value,timestamp,flag_a,flag_b"
    )]
    columns: Vec<String>,
    #[arg(long, default_value_t = 65536)]
    batch_rows: usize,
    #[arg(long, default_value_t = 1024)]
    memory_mib: usize,
    #[arg(long, default_value_t = 1e-8)]
    absolute_tolerance: f64,
    #[arg(long, default_value_t = 1e-6)]
    relative_tolerance: f64,
    #[arg(long, value_parser=["equal", "unequal"], default_value="equal")]
    nan_policy: String,
}

pub async fn execute(args: &Args, thread_budget: u16) -> super::Result<Value> {
    if args.output.exists() || !(16..=65536).contains(&args.memory_mib) {
        return Err(
            "INVALID_REQUEST: output must be new and memory_mib must be in [16,65536]".into(),
        );
    }
    // Each input is one already-ordered Parquet file. No hidden sort or fan-out.
    for path in [&args.left, &args.right] {
        if !path.is_file() || path.is_symlink() {
            return Err("INVALID_REQUEST: inputs must be regular local Parquet files".into());
        }
    }
    let limits = Limits {
        input_batch_rows: args.batch_rows,
        output_batch_rows: args.batch_rows,
        ..Default::default()
    };
    limits.validate()?;
    let comparison = FloatComparison {
        absolute: args.absolute_tolerance,
        relative: args.relative_tolerance,
        nan_policy: if args.nan_policy == "equal" {
            NanPolicy::Equal
        } else {
            NanPolicy::Unequal
        },
    };
    comparison.validate()?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(args.memory_mib << 20)))
        .build()?;
    let mut config = SessionConfig::new()
        .with_batch_size(args.batch_rows)
        .with_target_partitions(1);
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
    let projection: Vec<&str> = args.columns.iter().map(String::as_str).collect();
    let mut inputs = Vec::new();
    for path in [&args.left, &args.right] {
        inputs.push(
            ctx.read_parquet(
                path.to_str()
                    .ok_or("INVALID_REQUEST: non-UTF8 input path")?,
                ParquetReadOptions::default(),
            )
            .await?
            .select_columns(&projection)?
            .create_physical_plan()
            .await?,
        );
    }
    let plan =
        ReleaseDiffExec::try_new_numeric(inputs.remove(0), inputs.remove(0), limits, comparison)?;
    let stats = plan.stats();
    let parent = args
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    let stage = tempfile::NamedTempFile::new_in(parent)?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(args.batch_rows))
        .build();
    let mut writer = ArrowWriter::try_new(stage.reopen()?, plan.schema(), Some(properties))?;
    let mut counts = json!({"INSERT":0,"DELETE":0,"UPDATE":0});
    let started = Instant::now();
    let mut stream = plan.execute(0, ctx.task_ctx())?;
    while let Some(batch) = stream.try_next().await? {
        let changes = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("invalid change type")?;
        for change in changes.iter().flatten() {
            counts[change] = json!(counts[change].as_u64().unwrap() + 1);
        }
        // At most one returned output batch is retained by this sink.
        writer.write(&batch)?;
    }
    writer.close()?;
    let core_and_sink_ns = started.elapsed().as_nanos() as u64;
    let publish_started = Instant::now();
    stage.as_file().sync_all()?;
    stage.persist_noclobber(&args.output)?;
    std::fs::File::open(parent)?.sync_all()?;
    let publish_ns = publish_started.elapsed().as_nanos() as u64;
    let stats = stats.lock().unwrap().clone();
    Ok(
        json!({"status":"complete","profile":"ordered-numeric-five-column-v1",
        "counts":counts,"operator":stats,"comparison":comparison,
        "input_rows":stats.left_rows+stats.right_rows,"input_rows_per_second":
            (stats.left_rows+stats.right_rows) as f64 / (core_and_sink_ns as f64 / 1e9),
        "core_and_bounded_sink_ns":core_and_sink_ns,"publication_ns":publish_ns,
        "batch_rows":args.batch_rows,"thread_budget":thread_budget,"datafusion_partitions":1,
        "async_executor_threads":1,"maximum_blocking_threads":thread_budget-1,
        "output":args.output,"scope":"local ordered projection; no implicit sort; OS file cache excluded from memory budget"}),
    )
}
