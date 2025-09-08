import logging

from pathlib import Path

from logger import setup_logging

from stac_dc import STAC_DC

from env import env


class Main:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Main, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return

        env.set_app__project_root(Path(__file__).resolve().parent)

        setup_logging(env.get_app__project_root())
        self._logger = logging.getLogger(env.get_app__name())

        self._initialized = True

        self._logger.debug(f"Project root: {env.get_app__project_root()}")
        self._logger.info("App initialized")

    def main(self):
        stac_dc = STAC_DC()
        stac_dc.run()


if __name__ == "__main__":
    main = Main()
    main.main()
