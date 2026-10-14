import logging
import os
import re
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING

from spark.common.config import KAFKA_CONNECTOR, SPARK_VERSION, SparkConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)
HADOOP_VERSION = "3.4.1"
S3A_CONNECTOR = f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"
CLOUD_CONNECTOR = f"org.apache.spark:spark-hadoop-cloud_2.13:{SPARK_VERSION}"


def check_runtime() -> None:
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
    if not java:
        raise RuntimeError("Java 21 is required; set JAVA_HOME or put java on PATH")
    try:
        result = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(
            "Cannot run Java; check JAVA_HOME and java -version (requires 21)"
        ) from error
    output = result.stderr + result.stdout
    if result.returncode or not re.search(r'version "21(?:\.|\")', output):
        raise RuntimeError("ConfigStream Spark requires Java 21; check JAVA_HOME and java -version")
    try:
        installed = version("pyspark")
    except PackageNotFoundError as error:
        raise RuntimeError("PySpark is not installed; run make spark-setup") from error
    if installed != SPARK_VERSION:
        raise RuntimeError(
            f"Expected PySpark {SPARK_VERSION}, found {installed}; run make spark-setup"
        )


def create_session(
    config: SparkConfig, *, kafka: bool = True, from_submit: bool = False
) -> "SparkSession":
    check_runtime()
    from pyspark.sql import SparkSession

    runtime = config.runtime_dir.resolve()
    for name in ("tmp", "local", "ivy", "warehouse"):
        (runtime / name).mkdir(parents=True, exist_ok=True)
    if not from_submit:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    else:
        from pyspark import SparkConf, SparkContext

        SparkContext._ensure_initialized()
        submitted = SparkConf()
        mode = submitted.get("spark.submit.deployMode", "")
        driver_pod = bool(
            submitted.get("spark.kubernetes.driver.pod.name", "")
            and os.environ.get("SPARK_APPLICATION_ID")
            and os.environ.get("SPARK_DRIVER_BIND_ADDRESS")
        )
        if (
            not submitted.get("spark.master", "").startswith("k8s://")
            or mode not in {"cluster", "client"}
            or mode == "client"
            and not driver_pod
        ):
            raise RuntimeError("Native submission requires Kubernetes cluster mode")
    os.environ["SPARK_LOCAL_DIRS"] = str(runtime / "local")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["TMPDIR"] = str(runtime / "tmp")
    builder = (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.warehouse.dir", (runtime / "warehouse").as_uri())
        .config("spark.jars.ivy", str(runtime / "ivy"))
        .config("spark.driver.extraJavaOptions", f'-Djava.io.tmpdir="{runtime / "tmp"}"')
    )
    if not from_submit:
        builder = builder.master(config.master).config("spark.driver.memory", config.driver_memory)
    packages = [KAFKA_CONNECTOR] if kafka else []
    if config.lake is not None:
        packages.extend([S3A_CONNECTOR, CLOUD_CONNECTOR])
        builder = (
            builder.config(
                "spark.jars.excludes",
                "org.apache.hadoop:hadoop-azure,org.apache.hadoop:hadoop-cloud-storage,"
                "com.google.cloud.bigdataoss:gcs-connector",
            )
            .config(
                "spark.sql.sources.commitProtocolClass",
                "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
            )
            .config(
                "spark.sql.parquet.output.committer.class",
                "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
            )
        )
    if packages and not from_submit:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    LOGGER.info(
        "Starting Spark %s on %s (UTC)",
        SPARK_VERSION,
        "native Kubernetes submission" if from_submit else config.master,
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    if from_submit and not session.sparkContext.master.startswith("k8s://"):
        session.stop()
        raise RuntimeError("Native runtime unexpectedly selected a non-Kubernetes master")
    if session.version != SPARK_VERSION:
        session.stop()
        raise RuntimeError(f"Spark runtime must match connector version {SPARK_VERSION}")
    if config.lake is not None:
        if session._jvm.org.apache.hadoop.util.VersionInfo.getVersion() != HADOOP_VERSION:
            session.stop()
            raise RuntimeError("Hadoop runtime must match the pinned S3A connector")
        lake = config.lake
        options = {
            "fs.s3a.endpoint": lake.endpoint,
            "fs.s3a.endpoint.region": lake.region,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.connection.ssl.enabled": str(lake.endpoint.startswith("https:")).lower(),
            "fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
            "fs.s3a.access.key": lake.access_key,
            "fs.s3a.secret.key": lake.secret_key,
            "fs.s3a.committer.name": "magic",
            "fs.s3a.committer.magic.enabled": "true",
            "mapreduce.outputcommitter.factory.scheme.s3a": (
                "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
            ),
            "fs.s3a.buffer.dir": str(runtime / "tmp" / "s3a"),
            "fs.s3a.connection.timeout": "5s",
            "fs.s3a.connection.establish.timeout": "5s",
        }
        for name, value in options.items():
            session.sparkContext._jsc.hadoopConfiguration().set(name, value)
    return session
