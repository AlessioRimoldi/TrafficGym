Generating a SUMO Network from OpenStreetMap
============================================

This guide walks through converting an OpenStreetMap export into a full SUMO
simulation setup — network, routes, and configuration — using TrafficGym's
built-in transformations.

Overview
--------

The process involves four steps, each producing artefacts that feed into the next:

1. Upload your ``.osm.xml`` file as an artefact
2. **netconvert** — convert the OSM file into a SUMO ``.net.xml`` network
3. **randomTrips** — generate a ``.rou.xml`` route file from the network
4. **sumocfg** — generate a ``.sumocfg`` configuration file tying everything together
5. Create a Scenario from the resulting artefacts

.. note::

   All transformations are run as background tasks by a Celery worker. After
   submitting each one, monitor its progress under **Transform Requests** before
   proceeding to the next step.

Step 1 — Upload the OSM file
-----------------------------

Navigate to **Artefacts** in the top navigation bar and click **Add Artefact**.
Select your ``.osm.xml`` file and click **Create**. It will appear in the artefacts
table once uploaded.

You can export an OSM file for any area from
`openstreetmap.org <https://www.openstreetmap.org/export>`_ by selecting a bounding
box and clicking **Export**.

Step 2 — Convert to a SUMO network (netconvert)
------------------------------------------------

In the artefacts table, check the box next to your ``.osm.xml`` file, then click
**Transform**.

In the Transform modal, select **netconvert** and map your ``.osm.xml`` to the
``osm`` input. Click **Transform** to submit.

Once complete, a ``.net.xml`` file will appear in the artefacts list.

Step 3 — Generate routes (randomTrips)
---------------------------------------

Check the box next to the ``.net.xml`` artefact produced in the previous step and
click **Transform**.

Select **randomTrips** and map the ``.net.xml`` to the ``network`` input. Under
**Parameters**, set ``end_step`` to the desired simulation duration in seconds,
for example:

.. code-block:: json

   {
     "end_step": "3600"
   }

Click **Transform** to submit. Once complete, a ``.rou.xml`` file will appear in
the artefacts list.

Step 4 — Generate a configuration file (sumocfg)
-------------------------------------------------

Select both the ``.net.xml`` and ``.rou.xml`` artefacts and click **Transform**.

Select **sumocfg** and map the artefacts to the ``net_xml`` and ``rou_xml`` inputs
respectively.

Click **Transform** to submit. Once complete, a ``.sumocfg`` file will appear in
the artefacts list.

Step 5 — Create a Scenario
---------------------------

Navigate to **Scenarios** and click **Add Scenario**. Give the scenario a name, then
under **Existing Artefacts** select:

- your ``.net.xml``
- your ``.rou.xml``
- your ``.sumocfg``

Click **Create**.

On creation, TrafficGym automatically submits a **netpreview** transformation for the
``.net.xml`` file, rendering a visual preview of the road network. Once complete, the
preview image will appear on the scenario card.

.. note::

   The netpreview transformation runs asynchronously. If the scenario card shows a
   spinner, the preview is still being generated.

Next Steps
----------

With a scenario in place you are ready to define an experiment and submit a run
request. See :doc:`getting_started` for the full workflow.