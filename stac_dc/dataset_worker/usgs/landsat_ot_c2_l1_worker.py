import logging

from stac_dc.dataset_worker.usgs import LandsatWorker

from env import env as env

class LandsatOTC2L1Worker(LandsatWorker):
    def __init__(
            self,
            logger=logging.getLogger(env.get_app__name()),
            **kwargs
    ):
        super().__init__(
            logger=logger,
            **kwargs
        )