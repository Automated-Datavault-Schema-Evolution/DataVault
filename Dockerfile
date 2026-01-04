# ============================
# Base image
# ============================
FROM python:3.12-slim-bullseye

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

RUN groupadd -g ${APP_GID} ${APP_GROUP} || true && \
    useradd -m -u ${APP_UID} -g ${APP_GID} -d /home/${APP_USER} -s /bin/bash ${APP_USER}

# ============================
# Dirs used at runtime
# ============================
# Create data lake dirs (match code & logs) and ivy cache dir with correct ownership
RUN install -d -m 2775 -o ${APP_UID} -g ${APP_GID} \
      /data/raw_vault /data/bronze /data/checkpoints /data/spark/warehouse /tmp/.ivy2

# Silence root warning from pip, speed up Python
ENV PIP_ROOT_USER_ACTION=ignore \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ============================
# App setup
# ============================
WORKDIR /app

# Python deps (keep this before copying source to leverage cache)
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# App code + env (copy with correct ownership to avoid slow chown)
COPY --chown=${APP_UID}:${APP_GID} . /app
COPY --chown=${APP_UID}:${APP_GID} .env /app/.env.docker

# Keep Ivy happy and mirror env knobs
ENV ENV_TYPE=docker \
    SPARK_IVY_PATH=/tmp/.ivy2

# Drop privileges
USER ${APP_UID}:${APP_GID}

CMD ["python", "main.py"]
