# ─────────────────────────────────────────────────────────────────
# DQ Engine — Dockerfile
# Single image runs both FastAPI (port 8000) and Streamlit (port 8501)
# using a process supervisor (supervisord)
# ─────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# ── System dependencies ───────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    supervisor \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Working directory ─────────────────────────────────────────────
WORKDIR /app

# ── Install Python dependencies first (layer caching) ────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy project files ────────────────────────────────────────────
COPY . .

# ── Create required directories ───────────────────────────────────
RUN mkdir -p reports sample_data logs

# ── Copy supervisord config ───────────────────────────────────────
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# ── Expose ports ─────────────────────────────────────────────────
EXPOSE 8000 8501

# ── Health check ─────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# ── Start both services via supervisord ──────────────────────────
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]