import logging
import os

def get_config():
    MIN_PORT = int(os.getenv("MIN_PORT", 8000))
    MAX_PORT = int(os.getenv("MAX_PORT", 8002))
    PORTS = list(range(MIN_PORT, MAX_PORT + 1))
    BASE_URL = os.getenv("BASE_URL", "http://{service}:{port}")

    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

    return {
        'ports': PORTS,
        'base_url': BASE_URL,
        'min_port': MIN_PORT,
        'max_port': MAX_PORT,
        'redis_host': REDIS_HOST,
        'redis_port': REDIS_PORT
    }


def get_peers(port, service_name=None):
    env_peers = os.getenv("PEERS")
    if env_peers:
        return [p for p in env_peers.split(",") if f":{port}" not in p]

    config = get_config()
    peers = []
    for i, p in enumerate(config['ports']):
        if p != port:
            svc = service_name or f"lock_manager_{i}"
            peers.append(config['base_url'].format(service=svc, port=p))
    return peers


def setup_logger(port):
    logger = logging.getLogger(f"Node-{port}")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"%(asctime)s [PORT {port}] [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger
