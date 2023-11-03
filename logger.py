import json
import os


class Logger:
    def __init__(self, log_path: str = 'puts.log'):
        self.log_file = log_path

    def log_write(self, key, value):
        with open(self.log_file, 'a') as log:
            log_entry = f"{key},{json.dumps(value)}\n"
            print(log_entry)
            log.write(log_entry)

    def recover(self):
        print("Recovering...")
        operations = []
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as log:
                for line in log:
                    if line.strip():
                        key, value = line.strip().split(',', 1)
                        operations.append((key, json.loads(value)))
        return operations

    def clear_log(self):
        if os.path.exists(self.log_file):
            os.remove(self.log_file)