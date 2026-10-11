import logging

import uvicorn

from api.config import ApiConfig


def main() -> None:
    config = ApiConfig.from_env()
    logging.basicConfig(level="DEBUG" if config.log_level == "trace" else config.log_level.upper())
    uvicorn.run(
        "api.app:create_app",
        factory=True,
        host=config.host,
        port=config.port,
        log_level=config.log_level,
        access_log=False,
        server_header=False,
        proxy_headers=False,
    )


if __name__ == "__main__":
    main()
