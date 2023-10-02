from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import os

app = FastAPI()


class StorageInterface:
    storage_dir = "data_storage"

    @classmethod
    def _get_file_path(cls, key: str) -> str:
        return os.path.join(cls.storage_dir, key)

    @classmethod
    def get(cls, key: str) -> Optional[str]:
        file_path = cls._get_file_path(key)
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                return file.read()
        return None

    @classmethod
    def set(cls, key: str, value: str):
        os.makedirs(cls.storage_dir, exist_ok=True)
        file_path = cls._get_file_path(key)
        with open(file_path, 'w') as file:
            file.write(value)


@app.get("/keys/{key}")
def read_key(key: str):
    value = StorageInterface.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}


@app.put("/keys/{key}")
def write_key(key: str, value: str = Query(...)):
    StorageInterface.set(key, value)
    return {"status": "success"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
