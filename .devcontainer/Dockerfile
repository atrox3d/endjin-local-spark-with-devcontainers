FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

# change this to the target architecture
ARG JAVA_ARCH=arm64

# install JRE
# RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    openjdk-17-jre \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${JAVA_ARCH}

# install Spark
RUN --mount=type=cache,id=spark-download,target=/tmp/spark-cache \
    cd /tmp && \
    wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    cd / && \
    tar -xzf /tmp/spark-3.5.0-bin-hadoop3.tgz && \
    mv /spark-3.5.0-bin-hadoop3 /spark && \
    rm -rf /tmp/spark-3.5.0-bin-hadoop3.tgz

ENV SPARK_HOME=/spark
