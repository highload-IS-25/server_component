def parse_redis_conf(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith('#') or not line.strip():
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                key, value = parts
                config[key.strip()] = value.strip()
    return config
