FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY examples ./examples
COPY scripts ./scripts

# 持久化目录:SQLite 库与上传的 OCF 原件都落在 /data(由 compose 卷挂载)
RUN mkdir -p /data/files
ENV AVRO_DATA_DIR=/data \
    AVRO_DB_PATH=/data/avro_imports.db \
    PYTHONUNBUFFERED=1

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
