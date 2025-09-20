FROM python:3.12-slim-bullseye

# Java 11 is officially supported by Spark 3.x
RUN apt-get update && apt-get install -y \
    gcc \
    libglib2.0-0 \
    git \
    openssh-client \
    wget \
    openjdk-11-jre-headless \
    procps  \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Add GitHub to known_hosts for SSH cloning
RUN mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Install Python dependencies
COPY requirements.txt ./
RUN --mount=type=ssh pip install --upgrade pip
RUN --mount=type=ssh pip install -r requirements.txt

# (OPTIONAL) If you use 'spark-submit' or Spark master/worker:
# RUN wget https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
#     && tar -xzf spark-3.4.1-bin-hadoop3.tgz -C /opt \
#     && ln -s /opt/spark-3.4.1-bin-hadoop3 /opt/spark
# ENV SPARK_HOME=/opt/spark
# ENV PATH=$SPARK_HOME/bin:$PATH

COPY . /app
COPY .env.docker /app/.env.docker

ENV ENV_TYPE=docker
CMD ["python", "main.py"]