from trafficgym.engine.ports.adapter_factory import AdapterFactory
from trafficgym.engine.ports.simulation import SimulationPort, RunConfig
from trafficgym.engine.adapters.libsumo_adapter import LibsumoAdapter
from trafficgym.engine.adapters.fake_adapter import FakeAdapter, FakeStateDict


class LibsumoAdapterFactory(AdapterFactory):
    def create(self, config: RunConfig, step_length_ms: int) -> SimulationPort:
        return LibsumoAdapter(config, step_length_ms)


class FakeAdapterFactory(AdapterFactory):
    def __init__(self, intial_config: FakeStateDict | None):
        self._initial_config = intial_config

    def create(self, _: RunConfig, step_length_ms: int) -> SimulationPort:
        return FakeAdapter(step_length_ms, self._initial_config)
