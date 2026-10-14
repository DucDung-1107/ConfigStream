FROM ghcr.io/astral-sh/uv:0.12.21 AS uv
FROM spark:4.0.2-scala2.13-java21-python3-ubuntu@sha256:ddf38559b2520671eb73a66a0c43185cc9e6b015aa944fcac32e5e05f610edd5
USER root
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_LINK_MODE=copy UV_NO_CACHE=1
WORKDIR /opt/configstream
COPY pyproject.toml uv.lock README.md ./
COPY schemas schemas
COPY generator generator
COPY messaging messaging
COPY spark spark
COPY processing processing
COPY lake lake
COPY search search
COPY api api
COPY workloads workloads
COPY analytics analytics
COPY benchmarks benchmarks
COPY rules rules
RUN uv python install 3.12.13 && uv sync --locked --extra kafka --extra spark --extra lake --extra search --extra api --extra ris --extra dns \
    && rm -rf .venv/lib/python3.12/site-packages/pyspark/jars \
    && ln -s /opt/spark/jars .venv/lib/python3.12/site-packages/pyspark/jars
RUN /opt/spark/bin/spark-submit --master 'local[1]' \
    --conf spark.jars.ivy=/tmp/ivy --conf spark.ui.enabled=false \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1,org.apache.spark:spark-hadoop-cloud_2.13:4.0.2 \
    --exclude-packages org.apache.hadoop:hadoop-azure,org.apache.hadoop:hadoop-cloud-storage,com.google.cloud.bigdataoss:gcs-connector \
    --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.13-4.0.2.jar 1 \
    && cp /tmp/ivy/jars/*.jar /opt/spark/jars/ && rm -rf /tmp/ivy
ENV PATH=/opt/configstream/.venv/bin:/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=/opt/configstream/.venv/bin/python \
    PYTHONPATH=/opt/configstream
RUN mkdir /runtime && chmod 1777 /runtime
RUN apt-get update && apt-get install -y --no-install-recommends git=1:2.34.1-1ubuntu1.17 \
    && rm -rf /var/lib/apt/lists/*
COPY infra/k8s/application.py /opt/configstream/application.py
COPY infra/k8s/roles.py /opt/configstream/roles.py
COPY infra/k8s/integrity.py /opt/configstream/integrity.py
COPY tests/unit tests/unit
COPY tests/conftest.py tests/conftest.py
COPY tests/fixtures/phase16 tests/fixtures/phase16
COPY tests/fixtures/phase17 tests/fixtures/phase17
COPY tests/fixtures/phase12 tests/fixtures/phase12
USER 185
WORKDIR /opt/configstream
