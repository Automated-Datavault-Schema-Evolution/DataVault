# ============================
# Base image
# ============================
FROM python:3.12-slim-bullseye

# Match Spark 3.5.x default JDK; keep tools you used
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc \
      libglib2.0-0 \
      git \
      openssh-client \
      wget \
      procps \
      openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ============================
# Single runtime user (UID/GID 1001)
# ============================
ARG APP_UID=1001
ARG APP_GID=1001
ARG APP_USER=appuser
ARG APP_GROUP=appgroup

# Create group + user once (no duplicate 185:185 user)
RUN groupadd -g ${APP_GID} ${APP_GROUP} || true && \
    useradd -m -u ${APP_UID} -g ${APP_GID} -d /home/${APP_USER} -s /bin/bash ${APP_USER}

# ============================
# Dirs used at runtime
# ============================
# Create data lake dirs (match  code & logs) and ivy cache dir
    # make them writable by the runtime user
RUN mkdir -p /data/raw_vault /data/bronze /data/checkpoints /data/spark/warehouse /tmp/.ivy2 && \
    chown -R ${APP_UID}:${APP_GID} /data /tmp

# Silence root warning from pip
ENV PIP_ROOT_USER_ACTION=ignore PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# ============================
# App setup
# ============================
WORKDIR /app

# Python deps
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# App code + env
COPY . /app
COPY .env.docker /app/.env.docker

# Keep Ivy happy and mirror env knobs
ENV ENV_TYPE=docker \
    SPARK_IVY_PATH=/tmp/.ivy2
# (HOME can stay as the user's home; Spark sets -Duser.home=/tmp in code)

# Make sure runtime user owns the app dir too
RUN chown -R ${APP_UID}:${APP_GID} /app

# Drop privileges
USER ${APP_UID}:${APP_GID}

# Default command
CMD ["python", "main.py"]
