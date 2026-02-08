FROM python:3.10-slim

WORKDIR /app

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    apt-get clean

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY pipeline.py .
COPY profiling.py .

# Create folders
RUN mkdir -p /app/data /app/output

CMD ["python"]