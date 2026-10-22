import os
from pathlib import Path

def _load_env():
    candidates = [
        Path.cwd() / '.env',
        Path(__file__).resolve().parent.parent / '.env',
        Path(__file__).resolve().parent / '.env',
    ]
    for path in candidates:
        if path.is_file():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            k, v = line.split('=', 1)
                            k = k.strip()
                            v = v.strip().strip('"').strip("'")
                            if k not in os.environ:
                                os.environ[k] = v
                break
            except Exception:
                pass

_load_env()

configuration = {
    "AWS_ACCESS_KEY": os.getenv("AWS_ACCESS_KEY", "YourAccessKey"),
    "AWS_SECRET_KEY": os.getenv("AWS_SECRET_KEY", "YourSecretKey"),
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092"),
    "S3_BUCKET": os.getenv("S3_BUCKET", "sparkstreamingdata-rahil"),
}
