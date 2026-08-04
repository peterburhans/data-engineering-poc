FROM python:3.12-slim
WORKDIR /app
COPY services/mock-data-service/pyproject.toml services/mock-data-service/README.md ./
COPY services/mock-data-service/src ./src
COPY services/mock-data-service/tests ./tests
COPY libs/data_contracts /app/shared
ENV PYTHONPATH=/app/shared
RUN pip install --no-cache-dir -e '.[dev]'
USER 65532:65532
CMD ["python", "-m", "mock_data_service.main"]
