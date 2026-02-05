from python:3.9-slim

workdir /app

# Copy and install Python dependencies
COPY pyproject.toml .
COPY app/ ./app/
COPY prompts/ ./prompts/

# Install the package (production mode, not editable)
RUN pip install --no-cache-dir .

# Default command (override in docker-compose)
CMD ["python", "-m", "app.workers.scheduler"]
