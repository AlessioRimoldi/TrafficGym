Transformations
===============

Transformations are server-side operations that produce new
:class:`~trafficgym.interface.core.models.Artefact` objects from existing ones
(e.g. converting an OSM file to a SUMO network, or generating a network preview
image).

Transformation Registry
-----------------------

.. automodule:: trafficgym.engine.transformations.registry
   :members:
   :undoc-members:
   :show-inheritance:

Network Conversion
------------------

Wraps the SUMO ``netconvert`` tool.

.. automodule:: trafficgym.engine.transformations.netconvert
   :members:
   :undoc-members:
   :show-inheritance:

Network Preview
---------------

Generates a PNG preview of a ``.net.xml`` file using ``sumo-gui``.

.. automodule:: trafficgym.engine.transformations.netpreview
   :members:
   :undoc-members:
   :show-inheritance:

Inspection
----------

The inspect transformation feeds the graph builder's object ID dropdowns.
When a scenario is created, the platform runs ``inspect`` automatically against
the scenario's ``.net.xml`` file (and any ``.add.xml`` additional files).  It
parses the XML and extracts four sets of IDs:

* **tls_ids** — traffic light logic IDs (for actuators targeting ``trafficlight``)
* **edge_ids** — non-internal edge IDs (for edge-level queries)
* **lane_ids** — individual lane IDs (for lane-level queries)
* **detector_ids** — induction loop / E1 detector IDs from ``.add.xml`` files

The result is stored as a JSON artefact.  The graph builder reads it to
populate the object ID selector next to each observer and actuator node, so
users pick from real IDs rather than typing free-form strings.

.. automodule:: trafficgym.engine.transformations.inspect
   :members:
   :undoc-members:
   :show-inheritance:
