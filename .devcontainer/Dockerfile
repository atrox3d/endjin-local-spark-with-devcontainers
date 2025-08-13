FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

# change this to the target architecture
ARG JAVA_ARCH=arm64

# install JRE
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    openjdk-17-jre \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${JAVA_ARCH}

# install Spark
RUN --mount=type=cache,id=spark-download,target=/tmp/spark-cache \
    SPARK_TGZ="spark-3.5.0-bin-hadoop3.tgz" && \
    if [ ! -f "/tmp/spark-cache/${SPARK_TGZ}" ]; then \
        wget https://archive.apache.org/dist/spark/spark-3.5.0/${SPARK_TGZ} -O /tmp/spark-cache/${SPARK_TGZ}; \
    fi && \
    cp /tmp/spark-cache/${SPARK_TGZ} /tmp/${SPARK_TGZ} && \
    cd / && \
    tar -xzf /tmp/${SPARK_TGZ} && \
    mv /spark-3.5.0-bin-hadoop3 /spark && \
    rm /tmp/${SPARK_TGZ}

ENV SPARK_HOME=/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
