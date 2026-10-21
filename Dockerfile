FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    CHECKPOINT_PATH=/app/runtime/studio/checkpoints

WORKDIR /app
COPY requirements/base.txt requirements/base.txt
RUN python -m pip install --no-cache-dir -r requirements/base.txt
COPY . .
RUN python -m pip install --no-cache-dir --no-deps . \
    && useradd --create-home --uid 10001 studio \
    && mkdir -p /app/runtime/studio/checkpoints \
    && chown -R studio:studio /app/runtime

USER studio
ENV PGSYNC_STUDIO_HOST=0.0.0.0
EXPOSE 8180
CMD ["pgsync-ui"]
