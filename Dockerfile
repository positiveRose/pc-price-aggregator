FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Browsers already installed in base image — just link them
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

COPY . .

CMD ["python", "main.py", "--web"]
