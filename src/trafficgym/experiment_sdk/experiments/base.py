from abc import ABC, abstractmethod
from trafficgym.engine.client import driver


class Experiment(ABC):
    def __init__(self, run_handle: driver.RunHandle):
        self.run = run_handle

    @abstractmethod
    async def run_experiment(self) -> None: ...
