# Spark 4.1.1 + Delta 4.4.0 + S3A, Python 3.12 on driver and executors.
# Versions are recorded in docs/adr/ADR-002-versions.md.
FROM spark:4.1.1-scala2.13-java17-python3-ubuntu@sha256:6383b28979b5356455c51dff1153e603b182ee1b250938bf20261590dca0b1ad

ARG DELTA_VERSION=4.4.0
ARG HADOOP_AWS_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Python 3.12 to match the repo: PySpark requires the same minor version on driver and executors.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg software-properties-common; \
    add-apt-repository -y ppa:deadsnakes/ppa; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3.12; \
    apt-get purge -y software-properties-common; \
    apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/*; \
    python3.12 --version

# Delta + S3A jars, checksum-verified against Maven Central.
RUN set -eux; \
    cd /opt/spark/jars; \
    for coord in \
      "io/delta/delta-spark_4.1_2.13/${DELTA_VERSION}/delta-spark_4.1_2.13-${DELTA_VERSION}.jar" \
      "io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"; \
    do \
      f="$(basename "$coord")"; \
      curl -fsSL -o "$f" "${MAVEN}/${coord}"; \
      echo "$(curl -fsSL "${MAVEN}/${coord}.sha1")  ${f}" | sha1sum -c -; \
    done

ENV PYSPARK_PYTHON=/usr/bin/python3.12 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3.12

USER spark
