FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
COPY nidl ./nidl
COPY web ./web
RUN pip install --no-cache-dir '.[temporal]'
ENV NIDL_DB=/data/nidl.db
RUN mkdir -p /data
EXPOSE 8000
CMD ["uvicorn","nidl.api:app","--host","0.0.0.0","--port","8000"]
