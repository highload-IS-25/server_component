FROM python:3.10-slim-buster

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

VOLUME ["/app/data_storage"]

EXPOSE 8000

CMD ["uvicorn", "server_component:app", "--host", "0.0.0.0", "--port", "8000"]