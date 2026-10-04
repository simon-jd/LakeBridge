use arrow::array::StringArray;
use clap::{Parser, Subcommand};
use datafusion::{
    execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder},
    physical_plan::{ExecutionPlan, displayable, metrics::MetricValue},
    prelude::{SessionConfig, SessionContext},
};
use futures::TryStreamExt;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use releasequery_core::exec::{Limits, ReleaseDiffExec};
use releasequery_lakebridge::{
    Source,
    protocol::{DiffRequest, canonical, digest, hash, read_json},
    source_plan,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
mod numeric_cli;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Parser)]
#[command(
    version,
    about = "Complete exact diff of two explicitly pinned LakeBridge releases"
)]
struct Cli {
    /// Shared process budget: one async worker plus bounded blocking workers.
    #[arg(long, global=true, default_value_t=4, value_parser=clap::value_parser!(u16).range(2..=64))]
    threads: u16,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    NumericDiff(numeric_cli::Args),
    Inspect {
        request: PathBuf,
    },
    Explain {
        request: PathBuf,
    },
    Diff {
        request: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    request: DiffRequest,
    left_source: Option<Source>,
    right_source: Source,
    #[serde(default)]
    options: Options,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Options {
    limits: Limits,
    memory_bytes: usize,
    inputs_sorted: bool,
    sort_spill_reservation_bytes: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            memory_bytes: 512 << 20,
            inputs_sorted: false,
            sort_spill_reservation_bytes: 8 << 20,
        }
    }
}
fn physical_metrics(plan: &dyn ExecutionPlan, out: &mut Vec<Value>) {
    if let Some(metrics) = plan.metrics() {
        let compute = metrics
            .iter()
            .filter_map(|m| match m.value() {
                MetricValue::ElapsedCompute(t) => Some(t.value() as u64),
                _ => None,
            })
            .sum::<u64>();
        out.push(json!({"operator":plan.name(),"elapsed_compute_ns":compute,"scan_pull_wall_ns":metrics.iter().filter_map(|m|match m.value(){MetricValue::Time{name,time} if name=="scan_pull_wall"=>Some(time.value() as u64),_=>None}).sum::<u64>(),"spill_count":metrics.iter().filter_map(|m|match m.value(){MetricValue::SpillCount(c)=>Some(c.value() as u64),_=>None}).sum::<u64>(),"metrics":metrics.to_string()}));
    }
    for child in plan.children() {
        physical_metrics(child.as_ref(), out);
    }
}
struct Stage {
    path: PathBuf,
}
impl Stage {
    fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for Stage {
    fn drop(&mut self) {
        if self.path.exists() && !self.path.join("failure.json").exists() {
            let _ = std::fs::remove_file(self.path.join("manifest.json"));
            let _=write_sync(&self.path.join("failure.json"),br#"{"status":"failed","error":"cancelled or interrupted before atomic publication"}"#);
        }
    }
}
fn write_sync(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
async fn execute(cli: Cli) -> Result<Value> {
    if let Command::NumericDiff(args) = &cli.command {
        return numeric_cli::execute(args, cli.threads).await;
    }
    let started = Instant::now();
    let (path, output, explain) = match &cli.command {
        Command::Inspect { request } => (request, None, false),
        Command::Explain { request } => (request, None, true),
        Command::Diff { request, output } => (request, Some(output), false),
        Command::NumericDiff(_) => unreachable!(),
    };
    let raw = read_json(path)?;
    let input: Input = serde_json::from_value(raw)?;
    input.request.validate()?;
    input.options.limits.validate()?;
    if !(16 << 20..=64usize << 30).contains(&input.options.memory_bytes)
        || input.options.sort_spill_reservation_bytes > input.options.memory_bytes / 2
    {
        return Err("INVALID_REQUEST: invalid query memory / sort spill reservation".into());
    }
    let request = serde_json::to_value(&input.request)?;
    let request_hash = hash(&request)?;
    let source_value = json!({"left":input.left_source,"right":input.right_source});
    let source_hash = hash(&source_value)?;
    let stage = if let Some(output) = output {
        if output.exists() {
            return Err("CONFLICT: output already exists".into());
        }
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;
        Some(Stage {
            path: tempfile::Builder::new()
                .prefix(".release-query-")
                .tempdir_in(parent)?
                .keep(),
        })
    } else {
        None
    };
    let spill = tempfile::Builder::new()
        .prefix("release-query-spill-")
        .tempdir()?;
    let result: Result<Value> = async {
        let pool = Arc::new(FairSpillPool::new(input.options.memory_bytes));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool).with_temp_file_path(spill.path()).build()?;
        let mut config = SessionConfig::new()
            .with_batch_size(input.options.limits.input_batch_rows).with_target_partitions(if input.options.inputs_sorted { 1 } else { 2 });
        config.options_mut().execution.parquet.schema_force_view_types = false;
        config.options_mut().execution.sort_spill_reservation_bytes =
            input.options.sort_spill_reservation_bytes;
        let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
        let left = source_plan(
            &ctx, input.request.from_pin.as_ref(), input.left_source.as_ref(),
            &input.request.scope, &input.options.limits, input.options.inputs_sorted,
        ).await?;
        let right = source_plan(
            &ctx, Some(&input.request.to_pin), Some(&input.right_source),
            &input.request.scope, &input.options.limits, input.options.inputs_sorted,
        ).await?;
        let plan = ReleaseDiffExec::try_new(left, right, input.options.limits.clone())?;
        let physical = displayable(&plan).indent(true).to_string();
        let prepare_ns = started.elapsed().as_nanos() as u64;
        let Some(stage) = &stage else {
            return Ok(json!({
                "request_hash": request_hash, "source_hash": source_hash,
                "sources": source_value, "schema": format!("{}", plan.schema()),
                "physical_plan": if explain { Some(physical) } else { None },
                "verified_descriptors": true, "scanned_complete": false
            }));
        };
        let stats = plan.stats();
        let file = stage.path().join("changes.parquet");
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(input.options.limits.output_batch_rows)).build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&file)?, plan.schema(), Some(props),
        )?;
        let mut counts = json!({"INSERT": 0, "DELETE": 0, "UPDATE": 0});
        let mut stream = plan.execute(0, ctx.task_ctx())?;
        let mut writer_ns = 0u64;
        while let Some(batch) = stream.try_next().await? {
            let tick = Instant::now();
            let kinds = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or("INTEGRITY_ERROR: output type")?;
            for kind in kinds.iter().flatten() {
                counts[kind] = json!(counts[kind].as_u64()
                    .ok_or("INTEGRITY_ERROR: change type")? + 1);
            }
            writer.write(&batch)?;
            writer_ns += tick.elapsed().as_nanos() as u64;
        }
        let tick = Instant::now();
        writer.close()?;
        std::fs::File::open(&file)?.sync_all()?;
        writer_ns += tick.elapsed().as_nanos() as u64;
        let mut operators = Vec::new();
        physical_metrics(&plan, &mut operators);
        let stat = stats.lock().unwrap().clone();
        let file_sha256 = digest(&file)?;
        let total_ms = started.elapsed().as_millis() as u64;
        let mut manifest = json!({
            "status": "complete", "request": request, "request_hash": request_hash,
            "counts": counts,
            "files": [{"path": "changes.parquet", "sha256": file_sha256, "rows": stat.output_rows}],
            "backend": "rust-releasequery", "output_batch_rows": input.options.limits.output_batch_rows,
            "sources": source_value, "source_hash": source_hash, "physical_plan": physical,
            "metrics": {
                "total_ms": total_ms, "prepare_ns": prepare_ns, "output_write_ns": writer_ns,
                "operator": stat, "physical_operators": operators,
                "phase_note": "input_pull includes scan/sort/normalization; physical elapsed_compute is attributed CPU work, not additive wall time; total_ms is before manifest publication; RSS is measured externally"
            }
        });
        manifest["checksum"] = json!(hash(&manifest)?);
        write_sync(&stage.path().join("manifest.json"), &canonical(&manifest)?)?;
        std::fs::File::open(stage.path())?.sync_all()?;
        let output = output.unwrap();
        rustix::fs::renameat_with(
            rustix::fs::CWD, stage.path(), rustix::fs::CWD, output,
            rustix::fs::RenameFlags::NOREPLACE,
        )?;
        std::fs::File::open(output.parent().filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")))?.sync_all()?;
        Ok(manifest)
    }.await;
    if let (Err(err), Some(stage)) = (&result, stage) {
        let path = stage.path();
        let manifest = path.join("manifest.json");
        if manifest.exists() {
            std::fs::remove_file(manifest)?;
        }
        write_sync(
            &path.join("failure.json"),
            &serde_json::to_vec(
                &json!({"status":"failed","error":err.to_string(),"request_hash":request_hash}),
            )?,
        )?;
        eprintln!("Failed staging diagnostics: {}", path.display());
    }
    result
}
fn main() {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(usize::from(cli.threads - 1))
        .enable_all()
        .build()
        .expect("create bounded runtime");
    let result = runtime.block_on(async {
        tokio::select! {result=execute(cli)=>result,_=tokio::signal::ctrl_c()=>Err("CANCELLED: no complete output published".into())}
    });
    match result {
        Ok(value) => println!("{}", serde_json::to_string(&value).unwrap()),
        Err(e) => {
            eprintln!("{}", json!({"status":"failed","error":e.to_string()}));
            std::process::exit(1);
        }
    }
}
