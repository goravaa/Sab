FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

COPY main ./main
COPY mcp_server.py ./

ENV SAB_MCP_TRANSPORT=stdio \
    SAB_MCP_HOST=0.0.0.0 \
    SAB_MCP_PORT=8080

USER app

EXPOSE 8080

# Works only in streamable-http mode; Docker hosts should either expose HTTP
# or run the container with a TTY and drive stdio from the host CLI.
CMD ["python", "mcp_server.py"]
