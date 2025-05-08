FROM python:3.11-slim
RUN apt-get update && apt-get install -y build-essential libpq-dev
WORKDIR /code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
ENTRYPOINT ["python"]
