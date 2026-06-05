Case Study 1 — Ramp Metering
=============================

This case study compares four ramp metering strategies on a motorway on-ramp
scenario: no control, and three variants of the ALINEA feedback algorithm
(proportional, proportional-derivative, proportional-integral).  It
demonstrates the full TrafficGym workflow from scenario upload to analytics
comparison.

.. .. contents:: On this page
..    :local:
..    :depth: 2

Setting up the scenario
-----------------------

The ramp meter scenario is located in ``sumo_files/ramp_meter/`` and consists
of four files:

* ``ramp_meter.net.xml`` — the road network (motorway mainline with a merging
  on-ramp)
* ``ramp_meter.add.xml`` — three induction loop detectors
* ``ramp_meter.rou.xml`` — vehicle demand (stochastic departure times)
* ``ramp_meter.sumocfg`` — SUMO configuration tying the above together

Navigate to **Scenarios**, click **Upload**, and add all four files at once.
Name the scenario ``ramp_meter``.

On creation, two background processes run automatically:

**Network preview** — a PNG thumbnail of the road layout is generated and
shown on the scenario card.

**Inspection** — the platform parses the network and additional files and
extracts all IDs that can be used in the graph builder.  For this scenario
that produces:

* *TLS IDs*: ``TL0``
* *Detector IDs*: ``e1_0``, ``e1_1``, ``e1_2``

.. note::

   **[SCREENSHOT: scenario card after upload, showing network preview thumbnail
   and the inspect/preview status badges as COMPLETE]**

Finding the detector IDs with netedit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To understand which detector measures what, open netedit with the scenario
configuration::

   netedit --sumocfg sumo_files/ramp_meter/ramp_meter.sumocfg

In netedit, switch to **Inspect mode** (the magnifying glass icon, or press
``I``).  Click on any induction loop to see its attributes in the left-hand
panel, including its ID, lane, and position:

* ``e1_0`` — sits on the on-ramp lane (``E6.69_0``) at position 0 m.
  Measures vehicles entering the on-ramp queue.
* ``e1_1`` — on the mainline fast lane (``E6.69_2``) at 50 m downstream of the
  merge.  Measures post-merge mainline occupancy.
* ``e1_2`` — on the mainline slow lane (``E6.69_1``) at 50 m downstream.
  Same position as ``e1_1``, second lane.

ALINEA uses downstream occupancy — the congestion level on the mainline
*after* the merge — as its feedback signal.  Both ``e1_1`` and ``e1_2``
measure this, so we take the maximum across the two lanes.

Building the experiment graph
------------------------------

Open the ``ramp_meter`` scenario and click **New graph**.  Name the graph
``ramp_alinea_p_kr70`` (we will duplicate it later for other parameter sets).

The pipeline we are building:

.. code-block:: text

   Observer e1_1 ──┐
                   ├──► Max ──► Rolling Avg (15) ──► ALINEA-P ──► Cycle Actuator ──► TL0
   Observer e1_2 ──┘

.. note::

   **[SCREENSHOT: completed graph with all nodes connected]**

Step 1 — Observer nodes
~~~~~~~~~~~~~~~~~~~~~~~~

Add two **Observer** nodes:

* Domain ``inductionloop``, getter ``getLastStepOccupancy``, object ID
  ``e1_1``
* Domain ``inductionloop``, getter ``getLastStepOccupancy``, object ID
  ``e1_2``

Use ``getLastStepOccupancy`` rather than ``getLastIntervalOccupancy``.  The
interval getter averages over a fixed 300-second window (as configured in
``ramp_meter.add.xml``) and only updates at 300-second boundaries, which
causes sudden jumps in the signal at each reset.  The step getter returns
the occupancy measured during the most recent simulation step, giving a
continuous signal that the Rolling Average block can smooth gradually and
uniformly.

.. note::

   **[SCREENSHOT: observer node configuration panel showing domain/getter/id
   dropdowns]**

Step 2 — Max block
~~~~~~~~~~~~~~~~~~~

Add a **Max** block and connect both observer nodes into it.  This outputs the
greater of the two lane occupancies under the key ``occupancy``, giving a
conservative (worst-case) reading of mainline congestion.

Step 3 — Rolling Average (window 15)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add a **Rolling Avg** block.  Set **Window** to ``15``.  Connect the Max block
into it.  This smooths out short-lived fluctuations (vehicles bunching) before
the signal reaches the ALINEA controller, preventing over-reaction to noise.

Step 4 — ALINEA-P controller
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add an **ALINEA-P** block.  Key parameters:

* **Target occupancy** (``target_occupancy``) — the downstream occupancy
  setpoint, typically 20 %.  This is the level at which the mainline is well
  utilised but not congested.
* **Kr** — proportional gain.  Start with ``70`` (veh/h per %).  We will
  compare higher and lower values in the analytics section.
* **Saturation** — maximum upward correction per step.  Leave at the default.

Connect the Rolling Avg output into ALINEA-P.  The controller outputs
``meter_rate_veh_per_h``.

Step 5 — Cycle Actuator
~~~~~~~~~~~~~~~~~~~~~~~~

Add a **Cycle Actuator** block and connect ALINEA-P into it.  This converts
the continuous metering rate (veh/h) into a two-phase traffic light state
string (``G`` for green, ``r`` for red) timed to deliver the requested rate.
It only emits on phase transitions, so the downstream actuator is not called
every tick.

Step 6 — Actuator node
~~~~~~~~~~~~~~~~~~~~~~~

Add an **Actuator** node:

* Domain ``trafficlight``
* Setter ``setRedYellowGreenState``
* Object ID ``TL0``

Connect the Cycle Actuator output into it.  This applies the ``G``/``r`` state
string to the physical ramp meter signal each time the phase changes.

.. note::

   **[SCREENSHOT: full pipeline from both observers through Max → Rolling Avg
   → ALINEA-P → Cycle Actuator → TL0 actuator]**

Recording intermediate results
--------------------------------

Rather than only recording the final TLS command, we can log intermediate
signals to understand controller behaviour in the analytics view.

In the graph builder, each block node has a **Record** section.  Adding a
record entry causes that output value to be stored as a named subscription on
every tick it is emitted.

Add the following record entries:

* On the **Max** block — record output key ``occupancy`` as
  ``max_occupancy``.  This shows the raw worst-case lane reading before
  smoothing.
* On the **Rolling Avg** block — record output key ``occupancy`` as
  ``smoothed_occupancy``.  This is the signal the controller actually acts on.
* On the **ALINEA-P** block — record output key ``meter_rate_veh_per_h`` as
  ``meter_rate``.  This shows the instantaneous metering rate the controller
  is requesting.

.. note::

   **[SCREENSHOT: record section on ALINEA-P block with meter_rate entry
   filled in]**

Adding an unconnected observer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To record the actual TLS state for verification, add a standalone **Observer**
node that is *not* connected to any block:

* Domain ``trafficlight``
* Getter ``getRedYellowGreenState``
* Object ID ``TL0``

Because this observer is not wired into a pipeline it does not influence
control, but it still creates a subscription — every tick the current state
string (e.g. ``GGrr``) is logged under the fingerprint
``trafficlight.getRedYellowGreenState.TL0``.  This lets you verify in analytics
that the TLS is actually switching as commanded.

Setting up the phases
----------------------

Before saving, configure the run **phases** in the graph builder:

1. **Warm-up phase** — 300 s, no pipelines active.  Allows traffic to build up
   naturally before the controller is switched on.
2. **Controlled phase** — 3600 s (1 hour), the pipeline active.
3. (Optional) **Cool-down phase** — 300 s, no pipelines active.

Click **Save**.  The platform generates a Python experiment class and
registers it as ``ramp_alinea_p_kr70``.

Running the experiment
-----------------------

Quick verification with Open GUI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before committing to a full multi-seed run, do a single sanity-check run with
**Open GUI** enabled.  This launches ``sumo-gui`` on the worker machine so you
can watch the simulation in real time and confirm the ramp meter is switching
as expected.

.. note::

   The GUI opens on the **Celery worker machine**, not in your browser.  Use
   this on your local development machine only.

In the **Create Run Request** modal:

* Select scenario ``ramp_meter``, experiment ``ramp_alinea_p_kr70``
* Set reruns to ``1``
* Tick **Open GUI**
* Submit

Watch the ramp meter at junction ``TL0`` — you should see it cycling between
green and red according to mainline congestion.

.. note::

   **[SCREENSHOT: sumo-gui showing the ramp meter scenario mid-run with
   vehicles queued on the on-ramp]**

Full multi-seed run
~~~~~~~~~~~~~~~~~~~~

Once satisfied, submit a production run:

* Reruns: ``20``
* Leave seeds blank (random)
* Open GUI: off

This runs 20 independent seeds in parallel across Celery workers.  Each seed
produces different vehicle departure times, averaging out the stochastic
variation in demand.

Comparing different Kr values
-------------------------------

Duplicate the graph (or build a new one from scratch) and change only the
**Kr** parameter on the ALINEA-P block.  Save each variant with a name that
encodes the key parameter so they are easy to distinguish in analytics:

+---------------------------+-------+------+
| Experiment name           | Kr    | Type |
+===========================+=======+======+
| ``ramp_alinea_p_kr40``    | 40    | P    |
+---------------------------+-------+------+
| ``ramp_alinea_p_kr70``    | 70    | P    |
+---------------------------+-------+------+
| ``ramp_alinea_p_kr120``   | 120   | P    |
+---------------------------+-------+------+
| ``ramp_alinea_pd_kr70``   | 70    | PD   |
+---------------------------+-------+------+
| ``ramp_alinea_pi_kr70``   | 70    | PI   |
+---------------------------+-------+------+

Run each with 20 seeds.  You can submit all runs before any of them finish —
they queue in Celery and execute concurrently.

Analysing results
-----------------

Open **Analytics** from any of the run request pages.  Click **Add runs** to
overlay multiple run requests on the same chart.

Suggested subscriptions to compare:

* ``smoothed_occupancy`` — does the controller hold downstream occupancy near
  the 20 % setpoint?  Higher Kr should converge faster; too high may
  oscillate.
* ``meter_rate`` — how does the requested metering rate evolve over time?
  Smoother curves suggest better damping.
* ``trafficlight.getRedYellowGreenState.TL0`` — visual confirmation that the
  TLS is responding.

Set the aggregation mode to **avg** to average across all 20 seeds for each
run request.  This removes per-seed noise and reveals the underlying
controller behaviour.

.. note::

   **[SCREENSHOT: analytics chart with smoothed_occupancy for three Kr
   variants overlaid, avg mode, showing convergence differences]**

Downloading results
~~~~~~~~~~~~~~~~~~~~

Use the **Download CSV** button on the analytics page to export the aggregated
time series.  The CSV contains one column per subscription fingerprint per run
request, labelled by run request ID.  Import into your analysis tool of choice
to produce publication-quality plots or run statistical comparisons across
controller variants.
