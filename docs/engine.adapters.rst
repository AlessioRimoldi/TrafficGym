Adapters
========

Adapters implement :class:`~trafficgym.engine.ports.simulation.SimulationPort`
for different simulation backends.

LibSUMO Adapter
---------------

The production adapter backed by ``libsumo``.

.. automodule:: trafficgym.engine.adapters.libsumo_adapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

Fake Adapter
------------

An in-memory adapter for unit testing experiments without a running SUMO
process.  Pre-populate :attr:`~trafficgym.engine.adapters.fake_adapter.FakeAdapter.state`
with :class:`~trafficgym.engine.adapters.fake_adapter.FakeStateDictKey` entries
to control what queries return.

.. automodule:: trafficgym.engine.adapters.fake_adapter
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

Counting Adapter
----------------

A dry-run adapter that counts steps without simulating.  Used internally to
pre-compute :attr:`~trafficgym.interface.core.models.Experiment.total_steps`.

.. automodule:: trafficgym.engine.adapters.counting_adapter
   :members:
   :undoc-members:
   :show-inheritance:

