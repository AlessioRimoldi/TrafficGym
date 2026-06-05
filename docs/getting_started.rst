Getting Started
===============

This page walks through the full workflow: uploading a scenario, defining an
experiment, running the simulation, and analysing the results.

Step 1 — Upload a scenario
--------------------------

A **scenario** is a collection of SUMO files that describe the road network and
traffic demand.  At minimum it needs a ``.net.xml`` network file and a
``.sumocfg`` configuration file.  Additional ``.rou.xml`` route files and
``.add.xml`` additional files (detectors, traffic lights, etc.) can also be
included.

Navigate to **Scenarios** and use the upload form to add your files.  On
creation the platform runs two background transformations automatically:

* **Inspect** — parses the ``.net.xml`` and any ``.add.xml`` files to extract
  all TLS IDs, edge IDs, lane IDs, and detector IDs.  These populate the object
  ID dropdowns in the graph builder.
* **Network preview** — renders a PNG overview of the network, shown as a
  thumbnail on the scenario card.

Step 2 — Define an experiment
------------------------------

There are two ways to define an experiment.

Option A: Upload a Python file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An experiment is a Python file containing a single class that subclasses
:class:`~trafficgym.engine.experiment.Experiment` and implements ``run()``.
When you upload the file, the platform automatically extracts the class name
and uses it as the experiment name.

.. code-block:: python

   from trafficgym.engine.experiment import Experiment
   from trafficgym.engine.ports.simulation import SimulationPort

   class my_experiment(Experiment):
       def run(self, adapter: SimulationPort) -> None:
           self.subscribe(
               "occupancy",
               domain="inductionloop",
               getter="getLastStepOccupancy",
               object_id="e1_detector_0",
           )
           adapter.run_time(3600)

Subscriptions declared with
:meth:`~trafficgym.engine.experiment.Experiment.subscribe` are collected on
every tick and stored automatically.

Upload the ``.py`` file on the **Experiments** page.

Option B: Use the experiment graph builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The graph builder lets you visually wire together observer nodes, blocks
(controllers, aggregators, utilities), and actuator nodes without writing code.

1. Open a scenario and click **New graph**.
2. Add **observer** nodes — each one subscribes to a SUMO getter on every
   tick.  Pick the domain, getter, and object ID from the dropdowns populated
   by the inspect transformation.
3. Add **block** nodes — controllers and aggregators from the block palette.
   Input/output ports and configurable parameters are discovered automatically
   from each Python class (see :ref:`block-registry-graph-builder-introspection`).
4. Add **actuator** nodes — each one applies a SUMO setter when its connected
   block emits an output.
5. Connect nodes by dragging from an output port to an input port.
6. Organise the run into **phases** — each phase activates a set of pipelines
   for a fixed duration or until the network empties.
7. Click **Save** — the platform generates a Python
   :class:`~trafficgym.engine.experiment.Experiment` subclass from the graph
   and registers it as an experiment.

Writing controllers in code
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you upload a Python file and want to use a built-in controller, use
:meth:`~trafficgym.engine.ports.simulation.SimulationPort.controlled` — a
context manager that wires a controller into the simulation loop for the
duration of the ``with`` block.  On every tick the adapter runs:

.. code-block:: text

   observe(adapter, sim_time)  →  inputs dict
           ↓
   controller.step(adapter, inputs)  →  result dict
           ↓
   actuate(adapter, result)

``observe`` — reading from SUMO
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``observe`` receives the adapter and the current simulation time in seconds,
and must return a dict whose keys match what the controller expects as inputs.
Use :meth:`~trafficgym.engine.ports.simulation.SimulationPort.query` to read
any SUMO value:

.. code-block:: python

   observe=lambda a, _: {
       "occupancy": float(a.query("inductionloop", "getLastIntervalOccupancy", det_id, {}))
   }

The simulation time is available as ``t`` when the controller needs it —
:class:`~trafficgym.engine.control.controllers.StaticTLSController` is the
main example:

.. code-block:: python

   observe=lambda a, t: {"sim_time": t}

``actuate`` — writing back to SUMO
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``actuate`` receives the adapter and the dict returned by
``controller.step()``.  Use
:meth:`~trafficgym.engine.ports.simulation.SimulationPort.apply` to write a
value back to SUMO.

Controllers only emit output when something needs to change —
:class:`~trafficgym.engine.control.controllers.RampMeterController` for
example only returns ``{"program_id": ...}`` on a state transition and ``{}``
on every other tick.  Because ``actuate`` is called **every tick** regardless,
always guard on key presence before applying anything:

.. code-block:: python

   # Correct — guard on key presence
   actuate=lambda a, r: a.apply("trafficlight", "setProgram", tls_id, {"programID": r["program_id"]})
   if "program_id" in r else None

   # Wrong — KeyError on the majority of ticks where r == {}
   actuate=lambda a, r: a.apply("trafficlight", "setProgram", tls_id, {"programID": r["program_id"]})

Full example:

.. code-block:: python

   from trafficgym.engine.experiment import Experiment
   from trafficgym.engine.ports.simulation import SimulationPort
   from trafficgym.engine.control.controllers import RampMeterController

   class metered_experiment(Experiment):
       def run(self, adapter: SimulationPort) -> None:
           det_id = "e1_on_ramp"
           tls_id = "ramp_meter"

           self.subscribe("occupancy", "inductionloop", "getLastIntervalOccupancy", det_id)

           with adapter.controlled(
               RampMeterController(),
               observe=lambda a, _: {
                   "occupancy": float(a.query("inductionloop", "getLastIntervalOccupancy", det_id, {}))
               },
               actuate=lambda a, r: a.apply("trafficlight", "setProgram", tls_id, {"programID": r["program_id"]})
               if "program_id" in r else None,
           ):
               adapter.run_time(7200)
           # Controller is deregistered here.

You can alternate controlled and uncontrolled phases freely:

.. code-block:: python

   adapter.run_time(300)                                          # warm-up
   with adapter.controlled(ctrl, observe=obs_fn, actuate=act_fn):
       adapter.run_time(3600)                                     # controlled
   adapter.run_time(300)                                          # cool-down

For multiple simultaneous controllers use ``contextlib.ExitStack``:

.. code-block:: python

   import contextlib

   with contextlib.ExitStack() as stack:
       stack.enter_context(adapter.controlled(ctrl_a, observe=obs_a, actuate=act_a))
       stack.enter_context(adapter.controlled(ctrl_b, observe=obs_b, actuate=act_b))
       adapter.run_time(3600)

Step methods and recording
~~~~~~~~~~~~~~~~~~~~~~~~~~

Three methods advance the simulation — all invoke wired controllers and collect
subscriptions on every step:

* :meth:`~trafficgym.engine.ports.simulation.SimulationPort.run_time` — advance by a fixed number of seconds.
* :meth:`~trafficgym.engine.ports.simulation.SimulationPort.run_steps` — advance by a fixed number of steps.
* :meth:`~trafficgym.engine.ports.simulation.SimulationPort.run_until_empty` — run until no vehicles remain in the network.

Use :meth:`~trafficgym.engine.experiment.Experiment.record` to emit a value
not directly queryable from SUMO — for example a controller's internal state:

.. code-block:: python

   self.record("controller_state", controller.state.name)

Step 3 — Create a run request
------------------------------

From the **Scenarios** page click **Run**.  Select a scenario and an
experiment, then configure:

* **Step length** — simulation step size in milliseconds (default 1000 ms).
* **Number of reruns** — how many independent executions to launch, each with
  a different random seed.  Use this to average out stochastic demand.
* **Seeds** — optionally provide explicit comma-separated integer seeds instead
  of random ones.  The number of seeds overrides the rerun count.

Open GUI
~~~~~~~~

Checking **Open GUI** launches ``sumo-gui`` instead of headless ``sumo`` for
this run.  **The GUI opens on the Celery worker machine, not in your browser.**
This is intended for development and debugging on a machine where you have
physical access to the worker (e.g. your local workstation).  When Open GUI is
enabled the run is forced to a single execution regardless of the rerun count.

Step 4 — Monitor and verify
----------------------------

After submitting, the run request page shows a live progress bar for each
execution.  Open any execution to see the worker logs and subscription data
collected so far.

Start with a single rerun and a short duration to confirm the experiment runs
without errors and the subscriptions you expect are being recorded before
committing to a full multi-run batch.

Step 5 — Run with multiple reruns
----------------------------------

Once verified, re-submit with a higher rerun count (e.g. 10–30) to collect
statistically meaningful data.  Each rerun uses an independent random seed so
vehicle departure times vary across executions.

Step 6 — Analyse results
-------------------------

Open **Analytics** from the run request page.  Select a subscription
fingerprint and an aggregation mode:

* **avg** — average across all reruns at each simulation time step.  The most
  common choice when comparing controller strategies.
* **min** / **max** / **sum** — other aggregations over reruns.

The chart overlays all selected subscriptions on a single time-series plot,
allowing you to compare metrics across different run requests side by side.
