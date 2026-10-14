FROM ghcr.io/astral-sh/uv:0.12.21 AS uv
FROM spark:4.0.2-scala2.13-java21-python3-ubuntu@sha256:ddf38559b2520671eb73a66a0c43185cc9e6b015aa944fcac32e5e05f610edd5
USER root
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_LINK_MODE=copy
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
COPY infra/k8s/application.py /opt/configstream/application.py
RUN uv python install 3.12.13 && uv sync --locked --no-dev --extra kafka --extra spark --extra lake --extra search --extra api --extra ris --extra dns
RUN /opt/spark/bin/spark-submit --master 'local[1]' \
    --conf spark.jars.ivy=/tmp/ivy --conf spark.ui.enabled=false \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1,org.apache.spark:spark-hadoop-cloud_2.13:4.0.2 \
    --exclude-packages org.apache.hadoop:hadoop-azure,org.apache.hadoop:hadoop-cloud-storage,com.google.cloud.bigdataoss:gcs-connector \
    --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.13-4.0.2.jar 1 \
    && cp /tmp/ivy/jars/*.jar /opt/spark/jars/ && rm -rf /tmp/ivy
ENV PATH=/opt/configstream/.venv/bin:/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=/opt/configstream/.venv/bin/python \
    PYTHONPATH=/opt/configstream
RUN chmod -R a+rX /opt/configstream /opt/python && mkdir /runtime && chmod 1777 /runtime
USER 185
WORKDIR /opt/configstream
