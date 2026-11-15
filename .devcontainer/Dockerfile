# Development environment for the stock market ELT pipeline.
# Python 3.11 on Debian 12 (bookworm), plus Java 17 for Spark.
FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# Spark runs on the JVM, so PySpark needs Java. On bookworm, default-jdk is OpenJDK 17.
# The base image includes a Yarn apt source with an expired signing key, which breaks
# apt-get update. We don't use Yarn, so remove that source first.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python packages into the image, so every rebuild gives the same environment.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
