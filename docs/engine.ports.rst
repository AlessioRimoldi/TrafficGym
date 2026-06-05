Simulation Port
===============

:class:`~trafficgym.engine.ports.simulation.SimulationPort` is the abstract
interface between an experiment and the underlying SUMO simulator.  Every step
method, query, and apply call goes through this port.

.. automodule:: trafficgym.engine.ports.simulation
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

SUMO Domains
------------

Constants mapping SUMO getter names to their Python cast functions and setter
parameter keys.

.. automodule:: trafficgym.engine.ports.sumo_domains
   :members:
   :undoc-members:
   :show-inheritance:

