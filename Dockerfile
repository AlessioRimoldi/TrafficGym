# Stage 1: compile TypeScript
FROM node:22-slim AS js-builder
WORKDIR /build
COPY src/trafficgym/interface/package.json src/trafficgym/interface/package-lock.json ./
RUN npm ci
COPY src/trafficgym/interface/tsconfig.json ./
COPY src/trafficgym/interface/core/static/core/ts/ ./core/static/core/ts/
RUN npm run build


# Stage 2: Python runtime
FROM python:3.12-slim-bookworm

# X11 client libs required by sumo-gui for display forwarding
RUN apt-get update && apt-get install -y --no-install-recommends \
        libx11-6 libxext6 libsm6 libxrender1 libgl1 libglib2.0-0 \
        libxi6 libxrandr2 libxdamage1 libxfixes3 libxcursor1 libxcomposite1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Runtime Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Docs build dependencies (sphinx + theme)
COPY requirements-docs.txt .
RUN pip install --no-cache-dir -r requirements-docs.txt

# Install the trafficgym package so autodoc can introspect it
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .

# Build Sphinx HTML (autodoc mocks libsumo/django/celery — see docs/conf.py)
COPY docs/ docs/
RUN python -m sphinx -b html docs docs/_build/html

# Copy compiled JS from builder stage
COPY --from=js-builder /build/core/static/core/js/build/ \
     src/trafficgym/interface/core/static/core/js/build/

COPY sumo_files/ sumo_files/

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
