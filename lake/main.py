import argparse
import logging

from lake.client import ensure_bucket
from lake.config import LakeConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Initialize the local data-lake bucket")
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        config = LakeConfig.from_env()
        if not args.check_config:
            ensure_bucket(config)
            logging.info("Lake bucket ready: %s", config.bucket)
        return 0
    except Exception:
        logging.error("Lake initialization failed; check credentials, endpoint, and bucket access")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
