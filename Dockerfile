FROM python:3.10-slim-buster

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

VOLUME ["/app/data_storage"]

EXPOSE $STORAGE_PORT

CMD uvicorn server_component:app --host $STORAGE_HOST --port $STORAGE_PORT
