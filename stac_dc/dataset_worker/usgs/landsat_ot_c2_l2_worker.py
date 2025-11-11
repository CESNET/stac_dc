import logging

from stac_dc.dataset_worker.usgs import LandsatWorker

from env import env as env


class LandsatOTC2L2Worker(LandsatWorker):
    def __init__(
            self,
            logger=logging.getLogger(env.get_app__name()),
            **kwargs
    ):
        super().__init__(
            dataset="landsat_ot_c2_l2",
            logger=logger,
            **kwargs
        )
