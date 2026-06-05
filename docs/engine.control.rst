Controllers & Aggregators
=========================

Built-in blocks for the experiment graph builder and direct use in experiments.

Controllers
-----------

.. automodule:: trafficgym.engine.control.controllers
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

Aggregators & Utilities
-----------------------

.. automodule:: trafficgym.engine.control.aggregators
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. automodule:: trafficgym.engine.control.utils
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

.. _block-registry-graph-builder-introspection:

Block Registry & Graph Builder Introspection
--------------------------------------------

Every block available in the graph builder is a Python class decorated with
:func:`~trafficgym.engine.control.registry.block`.  The decorator runs at
import time and uses Python introspection to extract everything the UI needs —
no separate schema or configuration file is required.

**Ports** are read from the TypedDict annotations on ``step()``:

.. code-block:: python

   class MyInputs(TypedDict):
       occupancy: float   # → input port  {key: "occupancy", type: "float"}

   class MyOutputs(TypedDict, total=False):
       program_id: str    # → output port {key: "program_id", type: "str"}

   @block("My Controller")
   class MyController:
       def step(self, adapter, inputs: MyInputs) -> MyOutputs: ...

**Parameters** are read from ``__init__`` arguments that have default values:

.. code-block:: python

   def __init__(self, target_occupancy: float = 20.0, Kr: float = 70.0):
       ...
   # → two number params shown in the block's settings panel

Ports and params discovered this way automatically appear in the graph builder
without any additional registration.  The block's label (shown in the UI),
description (shown as a tooltip), and any special UI params that have no
matching ``__init__`` argument can be added via ``extra_params``:

.. code-block:: python

   @block("Static TLS", extra_params=[BlockParam("phase_rows", "phase_list", "Phases")])
   class StaticTLSController: ...

.. automodule:: trafficgym.engine.control.registry
   :members:
   :undoc-members:
   :show-inheritance:

.. _code-generation:

Code Generation
---------------

Generates a Python :class:`~trafficgym.engine.experiment.Experiment` subclass
from a graph spec saved in the UI.

.. automodule:: trafficgym.engine.control.codegen
   :members:
   :undoc-members:
   :show-inheritance:
