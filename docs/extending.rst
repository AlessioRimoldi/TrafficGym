Extending TrafficGym
====================

This page explains how to add new pipeline blocks and new SUMO TraCI getters/setters
so they appear in the graph builder UI without any changes to the frontend.

Writing a custom pipeline block
---------------------------------

Any Python class decorated with :func:`~trafficgym.engine.control.registry.block`
is automatically registered and available in the graph builder block palette.
The decorator uses Python introspection to extract everything the UI needs —
ports, parameters, label, and description — from the class itself.

.. code-block:: python

   from typing import TypedDict
   from trafficgym.engine.ports.simulation import SimulationPort
   from trafficgym.engine.control.registry import block

   class MyInputs(TypedDict):
       occupancy: float

   class MyOutputs(TypedDict, total=False):
       meter_rate_veh_per_h: float

   @block("My Controller")
   class MyController:
       """Short summary shown on the block in the graph builder.

       The full docstring is visible on hover. Use the first sentence to
       convey the core behaviour; use the rest for parameter guidance,
       caveats, or references.
       """

       def __init__(self, setpoint: float = 20.0) -> None:
           self.setpoint = setpoint

       def step(self, adapter: SimulationPort, inputs: MyInputs) -> MyOutputs:
           ...

This registers a block with:

* **Label** — ``"My Controller"`` (shown in the palette)
* **Description** — the class docstring; the first sentence is displayed on
  the block, the full text is shown on hover
* **Input port** — ``occupancy`` of type ``float`` (from ``MyInputs``)
* **Output port** — ``meter_rate_veh_per_h`` of type ``float`` (from ``MyOutputs``)
* **Parameter** — ``setpoint`` with default ``20.0`` (from ``__init__``)

Block ports
-----------

Input and output ports are derived from the TypedDict annotations on ``step()``:

* The ``inputs`` parameter must be annotated with a ``TypedDict`` subclass.
  Each field becomes an **input port** with its name and type.
* The return type must be a ``TypedDict`` subclass declared with
  ``total=False``.  Each field becomes an **output port**.  ``total=False``
  is important — it signals that the block may return only a subset of keys
  on any given tick (returning ``{}`` means no output this tick).

.. code-block:: python

   class Inputs(TypedDict):
       occupancy: float           # required input port

   class Outputs(TypedDict, total=False):
       program_id: str            # optional output — may be absent in {}
       meter_rate_veh_per_h: float

Block parameters
----------------

Parameters are discovered from ``__init__`` arguments that have **default
values**.  Arguments without defaults are not exposed (they cannot be set from
the UI).  Only ``float``, ``int``, and ``str`` types are supported.

.. code-block:: python

   def __init__(
       self,
       target_occupancy: float = 20.0,   # ✓ exposed as a number param
       label: str = "default",            # ✓ exposed as a string param
       window: int = 10,                  # ✓ exposed as a number param
   ) -> None:

Overriding and adding parameters with ``extra_params``
------------------------------------------------------

The ``@block`` decorator auto-generates a
:class:`~trafficgym.engine.control.registry.BlockParam` for each ``__init__``
argument that has a default value.  ``extra_params`` lets you either **add**
params that have no ``__init__`` equivalent, or **override** the auto-generated
widget type for an existing param.

A :class:`~trafficgym.engine.control.registry.BlockParam` has five fields:

.. code-block:: python

   BlockParam(
       name,       # str  — matches the __init__ kwarg name (or a codegen-only key)
       type,       # str  — widget type (see below)
       label,      # str  — human-readable label shown in the UI
       default,    # Any  — value pre-filled in the UI (optional)
       choices,    # list[str] | None — required when type == "select"
   )

The ``type`` field controls which widget is rendered in the block settings panel:

* ``"number"`` — numeric input (used automatically for ``float`` and ``int`` params)
* ``"string"`` — text input (used automatically for ``str`` params)
* ``"select"`` — dropdown; ``choices`` must be provided
* ``"phase_list"`` — phase table with state-string and duration columns

**Use case 1 — Override the widget type for an existing param**

By default, a ``str`` parameter gets a plain text input.  If the parameter
only accepts a fixed set of values, replace it with a ``"select"`` dropdown:

.. code-block:: python

   @block(
       "Constant",
       extra_params=[BlockParam("value_type", "select", "Type", "float", ["float", "int", "str"])],
   )
   class Constant:
       def __init__(self, value: str = "0", output_key: str = "value", value_type: str = "float") -> None:
           ...

Here ``value_type`` is already an ``__init__`` argument, so ``extra_params``
*replaces* its auto-generated text input with a dropdown showing the three
allowed types.  The ``name`` must exactly match the ``__init__`` kwarg.

**Use case 2 — Add a param with no ``__init__`` equivalent**

Some blocks require UI input that is not a simple scalar — for example,
:class:`~trafficgym.engine.control.controllers.StaticTLSController` needs a
table of traffic light phases.  The phases are stored as ``list[str]`` and
``list[int]``, which cannot be represented by a plain number or string widget.

.. code-block:: python

   @block("Static TLS", extra_params=[BlockParam("phase_rows", "phase_list", "Phases")])
   class StaticTLSController:
       def __init__(self, phases: list[str], durations: list[int]) -> None:
           ...

``phase_rows`` is not an ``__init__`` argument — it is a synthetic key that
the graph builder stores as a list of ``{state, duration}`` row objects.  The
:ref:`code generator <code-generation>` has a special case for
``StaticTLSController`` that reads ``phase_rows`` and converts it into the two
lists ``phases`` and ``durations`` that ``__init__`` expects.  When adding your
own ``"phase_list"`` param you would need a matching codegen case if it maps to
a non-trivial constructor argument.

Where to put the block
----------------------

Place your block class in one of the existing modules so the ``@block``
decorator fires at import time:

* :mod:`trafficgym.engine.control.controllers` — feedback controllers
* :mod:`trafficgym.engine.control.aggregators` — fan-in aggregators
* :mod:`trafficgym.engine.control.utils` — utility blocks (actuator converters,
  constants, renamers)

Or create a new module in ``trafficgym/engine/control/`` and import it in
``trafficgym/engine/control/__init__.py`` so it is loaded alongside the others.

Adding SUMO getters and setters
---------------------------------

The graph builder's domain and getter/setter dropdowns are driven entirely by
:mod:`trafficgym.engine.ports.sumo_domains`.  Adding a new entry here makes it
available in the UI and in code generation with no frontend changes required.

File location: ``src/trafficgym/engine/ports/sumo_domains.py``

Adding a new getter (observer)
--------------------------------

Getters are grouped under **observer domains** in ``OBSERVER_DOMAINS``.  Each
domain groups logically related getters for the same SUMO TraCI API object type.

To add a getter to an existing domain, append a
:class:`~trafficgym.engine.ports.sumo_domains.GetterDef` to its ``getters``
list:

.. code-block:: python

   "detector": {
       "domain": "inductionloop",
       "getters": [
           {"getter": "getLastStepOccupancy",     "type": "float", "output_key": "occupancy"},
           {"getter": "getLastStepVehicleNumber", "type": "int",   "output_key": "vehicle_count"},
           # Add your new getter here:
           {"getter": "getLastStepMeanSpeed",     "type": "float", "output_key": "mean_speed"},
       ],
   },

The fields are:

* ``getter`` — the exact SUMO TraCI API method name (e.g. ``getLastStepMeanSpeed``)
* ``type`` — Python type string used for port type matching; one of ``"float"``,
  ``"int"``, ``"str"``, ``"list[str]"``
* ``output_key`` — the key this getter produces in the observation dict; must
  match the input port key of any block you want to connect it to

To add an entirely new observer domain (a new SUMO TraCI API object type), add a
new entry to ``OBSERVER_DOMAINS``:

.. code-block:: python

   "vehicle": {
       "domain": "vehicle",
       "getters": [
           {"getter": "getSpeed",  "type": "float", "output_key": "speed"},
           {"getter": "getLaneID", "type": "str",   "output_key": "lane_id"},
       ],
   },

Adding a new setter (actuator)
--------------------------------

Setters are grouped under **actuator domains** in ``ACTUATOR_DOMAINS``.  To
add a setter to an existing domain, append a
:class:`~trafficgym.engine.ports.sumo_domains.SetterDef`:

.. code-block:: python

   "tls": {
       "domain": "trafficlight",
       "setters": [
           {"setter": "setProgram",             "sumo_param": "programID", "type": "str", "input_key": "program_id"},
           {"setter": "setRedYellowGreenState", "sumo_param": "state",     "type": "str", "input_key": "state"},
           # Add your new setter here:
           {"setter": "setPhase",               "sumo_param": "index",     "type": "int", "input_key": "phase"},
       ],
   },

The fields are:

* ``setter`` — the exact SUMO TraCI API method name
* ``sumo_param`` — the keyword argument name the SUMO TraCI API expects
  (e.g. ``setPhase(tlsID, index=2)`` → ``sumo_param: "index"``)
* ``type`` — Python type string
* ``input_key`` — the key this setter reads from the connected block's output
  dict; must match the output port key of the upstream block

.. note::

   ``input_key`` is also used by the :ref:`code generator <code-generation>`
   when producing the ``actuate`` call.  If you change it, re-save any graphs
   that use this setter so the generated experiment is updated.

Port type compatibility
-----------------------

The graph builder validates connections between nodes based on port types.
A connection is allowed if the output port type of the upstream node matches
the input port type of the downstream node.  When adding getters and setters,
use consistent type strings (``"float"``, ``"int"``, ``"str"``) to ensure
your new entries connect cleanly to existing blocks.
