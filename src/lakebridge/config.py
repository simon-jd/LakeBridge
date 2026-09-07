from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from lakebridge.common import LakeBridgeError, identifier

SPARK_VERSION = "3.5.9"
ICEBERG_VERSION = "1.10.2"
ARTIFACTS = {
    "iceberg-spark-runtime-3.5_2.12-1.10.2.jar": (
        "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.2"
    ),
    "postgresql-42.7.5.jar": "org/postgresql/postgresql/42.7.5",
    "iceberg-aws-bundle-1.10.2.jar": "org/apache/iceberg/iceberg-aws-bundle/1.10.2",
}
ARTIFACT_SHA256 = {
    "iceberg-spark-runtime-3.5_2.12-1.10.2.jar": (
        "c562d482dcd16b666e808b755469fbc4b2604284b41622c9d87e2c02b7cc6c5f"
    ),
    "postgresql-42.7.5.jar": ("69020b3bd20984543e817393f2e6c01a890ef2e37a77dd11d6d8508181d079ab"),
    "iceberg-aws-bundle-1.10.2.jar": (
        "a2395ab49bf87e971e5588db644194ac18e9c0e0b44a7ad5a422a97648a38415"
    ),
}


@dataclass(frozen=True)
class Settings:
    root: Path
    namespace: str = "saas"

    def __post_init__(self):
        object.__setattr__(self, "root", self.root.resolve())
        identifier(self.namespace)

    @property
    def warehouse(self) -> str:
        return os.environ.get("LAKEBRIDGE_WAREHOUSE", str(self.root / "warehouse"))

    @property
    def audit_dsn(self) -> str:
        return os.environ.get("LAKEBRIDGE_AUDIT_DSN", "")

    def table(self, name: str) -> str:
        return f"lake.{self.namespace}.{identifier(name)}"


def spark_session(settings: Settings):
    from pyspark.sql import SparkSession

    settings.root.mkdir(parents=True, exist_ok=True)
    event_dir = settings.root / "event-logs"
    event_dir.mkdir(exist_ok=True)
    jar_dir = Path(os.environ.get("LAKEBRIDGE_JARS", ".runtime/jars")).resolve()
    jdbc = os.environ.get("LAKEBRIDGE_CATALOG_URI")
    needed = list(ARTIFACTS) if settings.warehouse.startswith("s3://") else list(ARTIFACTS)[:2]
    if not jdbc:
        needed = needed[:1]
    jars = [jar_dir / name for name in needed]
    if any(not p.is_file() for p in jars):
        raise LakeBridgeError("Runtime jars missing. Run: python scripts/bootstrap.py")
    master = os.environ.get("SPARK_MASTER", "local[2]")
    if master.startswith("local"):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.master(master)
        .appName("LakeBridge")
        .config("spark.jars", ",".join(str(p) for p in jars))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.warehouse", settings.warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_PARTITIONS", "4"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_dir.as_uri())
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "spark.local.dir", os.environ.get("SPARK_LOCAL_DIRS", str(settings.root / "spark-tmp"))
        )
    )
    remote_options = {
        "SPARK_DRIVER_HOST": "spark.driver.host",
        "SPARK_DRIVER_BIND_ADDRESS": "spark.driver.bindAddress",
        "SPARK_DRIVER_PORT": "spark.driver.port",
        "SPARK_BLOCK_MANAGER_PORT": "spark.blockManager.port",
        "SPARK_EXECUTOR_MEMORY": "spark.executor.memory",
        "SPARK_EXECUTOR_CORES": "spark.executor.cores",
        "SPARK_CORES_MAX": "spark.cores.max",
    }
    for env, conf in remote_options.items():
        if os.environ.get(env):
            builder = builder.config(conf, os.environ[env])
    if os.environ.get("LAKEBRIDGE_S3A") == "1":
        extra = sorted(jar_dir.glob("hadoop-aws-*.jar")) + sorted(
            jar_dir.glob("aws-java-sdk-bundle-*.jar")
        )
        if len(extra) != 2:
            raise LakeBridgeError("S3A dependencies missing; run scripts/bootstrap_simulation.py")
        builder = (
            builder.config("spark.jars", ",".join(str(p) for p in jars + extra))
            .config("spark.hadoop.fs.s3a.endpoint", os.environ["LAKEBRIDGE_S3_ENDPOINT"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
            .config("spark.hadoop.fs.s3a.buffer.dir", "/scratch/s3a")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        )
    if jdbc:
        builder = (
            builder.config(
                "spark.sql.catalog.lake.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"
            )
            .config("spark.sql.catalog.lake.uri", jdbc)
            .config("spark.sql.catalog.lake.jdbc.user", os.environ["LAKEBRIDGE_CATALOG_USER"])
            .config(
                "spark.sql.catalog.lake.jdbc.password", os.environ["LAKEBRIDGE_CATALOG_PASSWORD"]
            )
        )
    else:
        builder = builder.config("spark.sql.catalog.lake.type", "hadoop")
    if settings.warehouse.startswith("s3://"):
        builder = (
            builder.config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lake.s3.path-style-access", "true")
            .config(
                "spark.sql.catalog.lake.client.region", os.environ.get("AWS_REGION", "us-east-1")
            )
        )
        if os.environ.get("LAKEBRIDGE_S3_ENDPOINT"):
            builder = builder.config(
                "spark.sql.catalog.lake.s3.endpoint", os.environ["LAKEBRIDGE_S3_ENDPOINT"]
            )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
