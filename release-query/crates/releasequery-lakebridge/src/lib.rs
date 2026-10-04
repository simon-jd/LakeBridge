//! Fixed release descriptors; export and native Iceberg scans are distinct adapters.
pub mod normalize;
pub mod protocol;
mod timer;
use datafusion::{
    common::Result,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, coalesce_partitions::CoalescePartitionsExec,
        empty::EmptyExec, sorts::sort::SortExec,
    },
    prelude::{ParquetReadOptions, SessionContext},
};
use futures::TryStreamExt;
use iceberg::{TableIdent, io::FileIO, table::StaticTable};
use iceberg_datafusion::table::IcebergStaticTableProvider;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use protocol::*;
use releasequery_core::{
    exec::{Limits, error, key_order},
    schema::{detail_schema, live_schema},
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Source {
    ParquetExport {
        manifest_path: PathBuf,
        manifest_hash: String,
    },
    IcebergNative {
        pin: ReleasePin,
        metadata_path: PathBuf,
        metadata_sha256: String,
        adapter: String,
    },
}
fn check(condition: bool, message: &str) -> Result<()> {
    if !condition {
        return Err(error(message));
    }
    Ok(())
}
fn local(path: &str) -> Result<PathBuf> {
    let path = path.strip_prefix("file://").unwrap_or(path);
    check(
        !path.contains("://"),
        "INVALID_REQUEST: native v1 supports local filesystem only",
    )?;
    Ok(PathBuf::from(path))
}
fn parquet(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<std::fs::File>> {
    check(
        path.is_file() && !path.is_symlink(),
        "DEPENDENCY_UNAVAILABLE: regular source file required",
    )?;
    Ok(ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path)?,
    )?)
}
pub async fn source_plan(
    ctx: &SessionContext,
    pin: Option<&ReleasePin>,
    source: Option<&Source>,
    scope: &Scope,
    limits: &Limits,
    sorted: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(pin) = pin else {
        check(
            source.is_none(),
            "INVALID_REQUEST: source supplied for empty baseline",
        )?;
        return Ok(Arc::new(EmptyExec::new(live_schema())));
    };
    let source =
        source.ok_or_else(|| error("INVALID_REQUEST: pinned source descriptor missing"))?;
    let frame = match source {
        Source::ParquetExport {
            manifest_path,
            manifest_hash,
        } => {
            let mut manifest = read_json(manifest_path)?;
            let recorded = manifest
                .as_object_mut()
                .ok_or_else(|| error("INVALID_REQUEST: export object"))?
                .remove("manifest_hash")
                .ok_or_else(|| error("INTEGRITY_ERROR: missing manifest hash"))?;
            check(
                recorded == *manifest_hash && hash(&manifest)? == *manifest_hash,
                "INTEGRITY_ERROR: export manifest hash changed",
            )?;
            check(
                serde_json::from_value::<ReleasePin>(manifest["pin"].clone())
                    .map_err(|e| error(e.to_string()))?
                    == *pin,
                "INTEGRITY_ERROR: export pin differs",
            )?;
            check(
                manifest["status"] == "complete" && manifest["schema"] == "detail.utc.v1",
                "INCOMPLETE_DIFF: incomplete export",
            )?;
            let files = manifest["files"]
                .as_array()
                .ok_or_else(|| error("INVALID_REQUEST: files array"))?;
            check(
                !files.is_empty(),
                "DEPENDENCY_UNAVAILABLE: empty export listing",
            )?;
            let mut paths = Vec::new();
            for f in files {
                let name = f["path"]
                    .as_str()
                    .ok_or_else(|| error("INVALID_REQUEST: export file name"))?;
                check(
                    Path::new(name).file_name().and_then(|v| v.to_str()) == Some(name)
                        && name != "."
                        && name != "..",
                    "INTEGRITY_ERROR: export path escaped registry",
                )?;
                let path = manifest_path.parent().unwrap_or(Path::new(".")).join(name);
                check(
                    digest(&path)? == f["sha256"],
                    "INTEGRITY_ERROR: export file checksum changed",
                )?;
                let reader = parquet(&path)?;
                check(
                    reader.schema().as_ref() == detail_schema().as_ref(),
                    "INCOMPATIBLE_SCHEMA: export schema changed",
                )?;
                check(
                    Some(reader.metadata().file_metadata().num_rows() as u64) == f["rows"].as_u64(),
                    "INTEGRITY_ERROR: export row count changed",
                )?;
                let s = path
                    .to_str()
                    .ok_or_else(|| error("INVALID_REQUEST: UTF8 file path required"))?
                    .to_owned();
                check(!paths.contains(&s), "INTEGRITY_ERROR: repeated export file")?;
                paths.push(s);
            }
            ctx.read_parquet(paths, ParquetReadOptions::default())
                .await?
        }
        Source::IcebergNative {
            pin: actual,
            metadata_path,
            metadata_sha256,
            adapter,
        } => {
            check(
                actual == pin && adapter == "lakebridge.spark-utc-us.v1",
                "INTEGRITY_ERROR: native source pin/adapter differs",
            )?;
            check(
                digest(metadata_path)? == *metadata_sha256,
                "INTEGRITY_ERROR: native metadata checksum changed",
            )?;
            let metadata = read_json(metadata_path)?;
            check(
                metadata["format-version"] == 2,
                "INCOMPATIBLE_SCHEMA: native adapter supports Iceberg v2 only",
            )?;
            check(
                metadata["schemas"].as_array().is_some_and(|s| s.len() == 1),
                "INCOMPATIBLE_SCHEMA: native schema evolution needs explicit adapter",
            )?;
            check(
                metadata["properties"]
                    .get("schema.name-mapping.default")
                    .is_none(),
                "INCOMPATIBLE_SCHEMA: name mapping not supported by this adapter",
            )?;
            let table_pin = pin.detail()?;
            let snapshot_id = table_pin
                .snapshot_id
                .parse::<i64>()
                .map_err(|_| error("INVALID_REQUEST: snapshot ID"))?;
            let fields = metadata["schemas"][0]["fields"]
                .as_array()
                .ok_or_else(|| error("INCOMPATIBLE_SCHEMA: native fields"))?;
            let expected = [
                ("tenant_id", 1, "string"),
                ("event_id", 2, "string"),
                ("event_time", 3, "timestamp"),
                ("units", 5, "decimal(18, 6)"),
                ("plan_id", 9, "string"),
                ("billable_units", 11, "decimal(18, 6)"),
                ("rule_version", 12, "string"),
                ("is_deleted", 7, "boolean"),
            ];
            for (name, id, kind) in expected {
                check(
                    fields
                        .iter()
                        .any(|f| f["name"] == name && f["id"] == id && f["type"] == kind),
                    "INCOMPATIBLE_SCHEMA: native field IDs/types require explicit adapter",
                )?;
            }
            let snapshot = metadata["snapshots"]
                .as_array()
                .and_then(|s| {
                    s.iter()
                        .find(|s| s["snapshot-id"].as_i64() == Some(snapshot_id))
                })
                .ok_or_else(|| error("NOT_FOUND: pinned snapshot missing from metadata"))?;
            // Conservative v1 feature boundary even though the upstream reader implements
            // several delete modes. Never silently ignore a delete manifest or task.
            for key in [
                "total-delete-files",
                "total-position-deletes",
                "total-equality-deletes",
            ] {
                check(
                    snapshot["summary"].get(key).is_none_or(|v| v == "0"),
                    "INCOMPATIBLE_SCHEMA: native delete files require export adapter",
                )?;
            }
            let ident = TableIdent::from_strs(table_pin.table.split('.'))
                .map_err(|e| error(e.to_string()))?;
            let table = StaticTable::from_metadata_file(
                metadata_path
                    .to_str()
                    .ok_or_else(|| error("INVALID_REQUEST: metadata path"))?,
                ident,
                FileIO::new_with_fs(),
            )
            .await
            .map_err(|e| error(e.to_string()))?
            .into_table();
            let scan = table
                .scan()
                .snapshot_id(snapshot_id)
                .build()
                .map_err(|e| error(e.to_string()))?;
            let mut tasks = scan.plan_files().await.map_err(|e| error(e.to_string()))?;
            let mut n = 0;
            while let Some(task) = tasks.try_next().await.map_err(|e| error(e.to_string()))? {
                n += 1;
                check(
                    n <= 200_000,
                    "RESOURCE_EXHAUSTED: too many native scan tasks",
                )?;
                check(
                    task.deletes.is_empty(),
                    "INCOMPATIBLE_SCHEMA: native delete files require export adapter",
                )?;
                let reader = parquet(&local(&task.data_file_path)?)?;
                let descriptor = reader.metadata().file_metadata().schema_descr();
                for (name, id, _) in expected {
                    check(
                        descriptor.columns().iter().any(|c| {
                            c.name() == name
                                && c.self_type().get_basic_info().has_id()
                                && c.self_type().get_basic_info().id() == id
                        }),
                        "INCOMPATIBLE_SCHEMA: Parquet field IDs missing or changed",
                    )?;
                }
            }
            let provider =
                IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                    .await
                    .map_err(|e| error(e.to_string()))?;
            ctx.read_table(Arc::new(provider))?
        }
    };
    let names = detail_schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    let mut plan: Arc<dyn ExecutionPlan> = Arc::new(timer::ScanTimerExec::new(
        frame.select_columns(&names)?.create_physical_plan().await?,
    ));
    if plan.output_partitioning().partition_count() != 1 {
        plan = Arc::new(CoalescePartitionsExec::new(plan));
    }
    // 'sorted' is a service-supplied assertion, checked again by NormalizeExec across
    // all files/batches. Multi-file concatenation cannot silently masquerade as order.
    if !sorted {
        plan = Arc::new(SortExec::new(key_order(), plan));
    }
    Ok(Arc::new(normalize::NormalizeExec::try_new(
        plan,
        scope.clone(),
        limits.clone(),
    )?))
}
