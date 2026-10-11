# Stage 1: Build frontend
FROM node:20-alpine AS frontend
WORKDIR /build
COPY frontend/package.json ./
RUN npm install --legacy-peer-deps
COPY frontend/ .
RUN npm run build

# Stage 2: Python runtime
FROM python:3.11-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt --no-cache-dir
COPY backend/main_s3.py backend/trim_parquet.py backend/download_state.py backend/init_minio.py ./
COPY --from=frontend /build/dist ./static
COPY backend/mock_data ./mock_data
CMD ["sh", "-c", "set -e; if [ \"${INIT_DEMO_DATA:-0}\" = 1 ]; then python init_minio.py; fi; exec uvicorn main_s3:app --host 0.0.0.0 --port 8000"]