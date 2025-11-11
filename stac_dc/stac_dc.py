import logging

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from stac_dc.dataset_orchestrator import DatasetOrchestrator

from stac_dc.dataset_worker import *
from stac_dc.dataset_worker.aoi import *

from env import env


class STAC_DC:
    _orchestrators: List[DatasetOrchestrator] = []

    def __init__(
            self,
            logger: logging.Logger = logging.getLogger(env.get_app__name())
    ):
        self._logger: logging.Logger = logger

        self._prepare_orchestrators()

    def _prepare_orchestrators(self):
        self._orchestrators = []

        datasets_aios = env.get_all_datasets_aios()

        orchestrators: List[DatasetOrchestrator] = []

        for dataset, aoi in datasets_aios:
            if dataset not in workers_map:
                raise ValueError(f"Unknown dataset '{dataset}', no corresponding worker defined!")

            if aoi not in aois_map:
                raise ValueError(f"Unknown area of interest '{aoi}', no corresponding area of interest defined!")

            worker = workers_map[dataset]
            orchestrators.append(DatasetOrchestrator(worker=worker(aoi=aois_map[aoi])))

        self._orchestrators.extend(orchestrators)

    def run(self):
        self._logger.info(f"Starting STAC_DC with {len(self._orchestrators)} orchestrators")

        with ThreadPoolExecutor(max_workers=len(self._orchestrators)) as executor:
            futures = {
                executor.submit(orchestrator.execute): orchestrator
                for orchestrator in self._orchestrators
            }

            for future in as_completed(futures):
                orchestrator = futures[future]

                try:
                    future.result()
                    self._logger.info(f"[{orchestrator.get_worker().__class__.__name__}] Orchestrator finished")

                except Exception as e:
                    self._logger.error(
                        f"[{orchestrator.get_worker().__class__.__name__}] Orchestrator thread failed: {e}",
                        exc_info=True
                    )
