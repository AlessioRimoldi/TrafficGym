Case Study 3 — Using a File-Based Experiment
============================================

This case study walks through controlling more complex scenarios using a file-based experiment.
The experiment uses six green wave controllers, each offset to run concurrent left-to-right 
and bottom-to-top green waves. Demand in the scenario follows these same directions.

By observing ``example_experiments/megawave.py``, users can see why using file-based experiments 
can be easier than using the experiment graph builder.

Overview
--------

The process involves three steps.

1. Add the ``grid`` scenario.
2. Upload the ``megawave.py`` experiment.
3. Create a Run Request

Step 1 — Adding the Grid Scenario
---------------------------------

The grid scenario is located in ``sumo_files/grid/`` and consists
of four files:

* ``ramp_meter.net.xml`` — the road network (motorway mainline with a merging
  on-ramp)
* ``ramp_meter.add.xml`` — three induction loop detectors
* ``ramp_meter.rou.xml`` — vehicle demand (stochastic departure times)
* ``ramp_meter.sumocfg`` — SUMO configuration tying the above together

Navigate to **Scenarios**, click **Upload**, and add all four files at once.
Name the scenario ``grid``.

On creation, two background processes run automatically:

**Network preview** — a PNG thumbnail of the road layout is generated and
shown on the scenario card.

**Inspection** — the platform parses the network and additional files and
extracts all IDs that can be used in the graph builder.


.. note::

   **[SCREENSHOT: scenario card after upload, showing network preview thumbnail
   and the inspect/preview status badges as COMPLETE]**

Step 2 — Uploading the ``megawave.py`` Experiment
-------------------------------------------------

Navigate to the Experiments tab. Select Add Experiment \\> import from file, then 
navigate to ``example_experiments`` and upload ``megawave.py``.

The file should now appear as a file-based experiment.

Step 3 — Creating a Run Request
-------------------------------

Click the play button on the recently uploaded experiment card. Ensure the correct 
``grid`` scenario is selected.

Edit settings as required. (view in SUMO GUI) to see some cool waves.
