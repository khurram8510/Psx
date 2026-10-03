# syntax=docker/dockerfile:1
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    KMI30_DB_PATH=/data/kmi30.db \
    TZ=Asia/Karachi
RUN useradd --system --uid 10001 --no-create-home app \
 && mkdir -p /data && chown 10001:10001 /data
COPY --from=build /install /usr/local
USER 10001
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=4)"]
ENTRYPOINT ["kmi30", "--json-logs"]
CMD ["run"]
