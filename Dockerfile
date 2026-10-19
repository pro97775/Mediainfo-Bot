# syntax=docker/dockerfile:1
# =============================================================================
# Stage 1: Builder — install WZGram (WarpCrypto + uvloop) & C extensions in venv
# =============================================================================
FROM python:3.11-slim-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-compile -r requirements.txt \
    && pip uninstall -y setuptools wheel \
    && find /opt/venv -type d -name "__pycache__" -exec rm -rf {} + \
    && find /opt/venv -type d -name "tests" -exec rm -rf {} + \
    && find /opt/venv -type f -name "*.pyc" -delete

# =============================================================================
# Stage 2: Runtime — minimal image with mediainfo, ffprobe, and non-root user
# =============================================================================
FROM python:3.11-slim-bookworm AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONOPTIMIZE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PATH="/opt/venv/bin:$PATH" \
    TMP_DIR=/app/downloads \
    SESSION_DIR=/app/session \
    PORT=8080

# Install only runtime media probing tools and strip unnecessary binaries/docs
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        mediainfo \
        git \
        ca-certificates \
    && rm -f /usr/bin/ffplay /usr/bin/ffmpeg \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/apt/* \
        /usr/share/doc/* \
        /usr/share/man/* \
        /usr/share/info/* \
        /usr/share/locale/*

# Copy pre-built virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Create unprivileged runtime user and persistent directories
RUN groupadd -g 1000 appuser \
    && useradd -u 1000 -g appuser -d /app -s /sbin/nologin appuser \
    && mkdir -p /app/downloads /app/session \
    && chown -R appuser:appuser /app /opt/venv

WORKDIR /app
COPY --chown=appuser:appuser config.py dc_stream.py main.py requirements.txt ./

USER appuser

EXPOSE 8080

# Lightweight native Python healthcheck (avoids installing curl and libcurl)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\", 8080)}/', timeout=4)" || exit 1

CMD ["python", "main.py"]
