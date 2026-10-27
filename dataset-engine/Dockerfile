FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SAB_WORKSPACE=/app/config/workspace.yaml

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY pyproject.toml ./
COPY api ./api
COPY main ./main
RUN python -m pip install --upgrade pip \
    && python -m pip install -r api/requirements.txt

COPY --chown=app:app config/workspace.example.yaml ./config/workspace.example.yaml

USER app
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:8000/health', timeout=3)"

CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
