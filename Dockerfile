ARG IMAGE_VARIANT=slim-bullseye
ARG OPENJDK_VERSION=11
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT}

# Re-declare arguments so they are available in this stage
ARG OPENJDK_VERSION

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y openjdk-${OPENJDK_VERSION}-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy ONLY requirements.txt first
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

CMD ["python", "main.py"]