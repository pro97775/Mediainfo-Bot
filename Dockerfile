FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install system dependencies required for media probing
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        mediainfo \
        git \
        curl \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# Install Python dependencies first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Directory used for the temporary probe chunks and the pyrogram session
ENV TMP_DIR=/app/downloads \
    SESSION_DIR=/app/session
RUN mkdir -p /app/downloads /app/session


# Koyeb / platform-assigned port for the aiohttp health check server
ENV PORT=8080
EXPOSE 8080


# Container-level health check hitting the aiohttp endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:${PORT}/ || exit 1

CMD ["python", "main.py"]
