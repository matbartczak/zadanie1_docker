FROM python:3.11-slim

# 1. Set environment variables (better logging)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
    
# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run the app
CMD ["python", "weather_app.py"]
