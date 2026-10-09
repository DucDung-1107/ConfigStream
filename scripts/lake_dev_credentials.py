import os
import secrets
from pathlib import Path


def main() -> None:
    path = Path(".cache/minio.env")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        print("Keeping existing .cache/minio.env")
    else:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(
                f"MINIO_ROOT_USER=local-{secrets.token_hex(8)}\n"
                f"MINIO_ROOT_PASSWORD={secrets.token_hex(24)}\n"
            )
        print("Created private local credentials in .cache/minio.env")
    print("Load in your shell: set -a; . .cache/minio.env; set +a")


if __name__ == "__main__":
    main()
