Data Model
==========

This page documents the core Django models that underpin TrafficGym's data layer.
The design follows a few overarching principles:

- **Immutability** — most objects (artefacts, scenarios, experiments, run requests) are
  write-once. Mutation raises ``ValueError``, which prevents accidental data corruption
  and makes provenance tracking straightforward.
- **Content-addressing** — artefacts and experiments are identified by SHA-256 hashes of
  their content, so duplicate uploads are deduplicated automatically.
- **Provenance** — every file produced by the system is linked back to the inputs that
  generated it via :class:`TransformationRequest`.

----

Artefacts
---------

.. class:: Artefact

   A single immutable file stored by the platform. Artefacts are the atomic unit of
   data — every uploaded file, generated preview, and simulation output is an
   ``Artefact``.

   The primary key is the SHA-256 hash of the file contents, computed on first save.
   This means uploading the same file twice yields one ``Artefact``, not two.

   **Fields**

   - ``sha256`` — SHA-256 digest of the file, used as the primary key.
   - ``file`` — the underlying file, stored under ``artefacts/``. The filename on disk
     is ``{original_name}_{sha256}`` to avoid collisions.
   - ``original_name`` — the filename as uploaded by the user.
   - ``type`` — an optional label describing the artefact's role (e.g. ``net.xml``,
     ``net_preview``).
   - ``created_at`` — timestamp of upload.
   - ``metadata`` — arbitrary JSON for extension without schema changes.

   **Immutability**

   Calling ``save()`` on an existing ``Artefact`` raises ``ValueError``. Artefacts
   are never modified in place; if content changes, a new artefact with a different
   hash is created.

   **Relationships**

   - ``scenarios`` — the :class:`Scenario` objects that include this artefact
     (reverse of ``Scenario.artefacts``).
   - ``experiments`` — :class:`Experiment` objects whose source file is this artefact.
   - ``derivatives`` — :class:`TransformationInput` bindings where this artefact was
     used as an input to a transformation.
   - ``provenance`` — :class:`TransformationOutput` bindings recording which
     transformation produced this artefact.

----

Scenarios
---------

.. class:: Scenario

   A named collection of artefacts that together define a simulation network. Typically
   this includes a ``.net.xml`` network file, a ``.rou.xml`` route file, a
   ``.sumocfg`` configuration, and optionally additional files.

   A scenario is the context in which experiments are run. It is immutable after
   creation — artefacts cannot be removed, and no artefacts can be added once run
   requests exist against the scenario.

   **Fields**

   - ``id`` — UUID primary key.
   - ``name`` — unique human-readable name.
   - ``created_at`` — creation timestamp.
   - ``artefacts`` — many-to-many to :class:`Artefact`.

   **Properties**

   - ``image_transformation_request`` — finds the most recent ``netpreview``
     :class:`TransformationRequest` for this scenario's ``.net.xml`` file, if one
     exists. Used to show a visual preview of the network.
   - ``image_artefact`` — the first output artefact of the above transformation
     request, i.e. the rendered preview image.
   - ``image_url`` — the URL of the preview image, or an empty string.
   - ``compute_sha256()`` — computes a hash over the sorted SHA-256 hashes of all
     constituent artefacts. Used as the authoritative identity of the scenario's
     content when creating a :class:`RunRequest`.

   **Immutability**

   ``save()`` raises ``ValueError`` if called on an existing scenario. The M2M signal
   :func:`prevent_scenario_artefact_mutation` enforces artefact immutability after
   run requests exist.

----

Experiments
-----------

.. class:: Experiment

   A versioned, content-addressed Python experiment definition. An experiment is a
   class that subclasses ``trafficgym.engine.experiment.Experiment`` and implements
   a ``run()`` method.

   Each upload of an experiment file creates a new version. The SHA-256 of the
   source artefact serves as the primary key, so uploading the same file twice yields
   the same experiment record.

   **Fields**

   - ``sha256`` — SHA-256 of the source artefact, used as primary key.
   - ``name`` — the experiment's declared name.
   - ``version`` — auto-incremented per name on first save.
   - ``artefact`` — FK to the :class:`Artefact` containing the Python source.
   - ``total_steps`` — number of simulation steps, determined at registration time
     by running the experiment through a counting adapter.

   **Immutability**

   ``save()`` raises ``ValueError`` on existing records.

----

Run Requests
------------

.. class:: RunRequest

   A request to execute a specific :class:`Experiment` against a specific
   :class:`Scenario`, optionally with simulation parameters and multiple reruns.

   Run requests are dispatched to Celery workers and transition through the
   :class:`RunStatus` lifecycle.

   **Fields**

   - ``id`` — UUID primary key.
   - ``scenario`` — FK to :class:`Scenario`.
   - ``experiment`` — FK to :class:`Experiment`.
   - ``simulation_parameters`` — JSON dict of parameters passed to the experiment.
   - ``status`` — current lifecycle state (see :class:`RunStatus`).
   - ``rerun_count`` — number of times to execute this request (for statistical
     replication).
   - ``open_gui`` — whether to open the SUMO GUI during execution (Linux only).
   - ``step_length_ms`` — wall-clock duration of each simulation step in milliseconds.
   - ``run_signature`` — SHA-256 over scenario content hash, experiment hash, and
     simulation parameters. Identifies a logically identical request for caching.
   - ``created_at``, ``started_at``, ``finished_at`` — lifecycle timestamps.

   **Immutability**

   Only ``status``, ``started_at``, ``finished_at``, and ``worker_id`` may be updated
   after creation. Any other update raises ``ValueError``. ``update_fields`` must
   always be passed explicitly.

.. class:: RunStatus

   ``TextChoices`` enum describing the lifecycle of a :class:`RunRequest` or
   :class:`RunExecution`:

   - ``PENDING`` — queued, not yet picked up.
   - ``PREPARING`` — worker is setting up the environment.
   - ``RUNNING`` — simulation is actively executing.
   - ``COMPLETE`` — finished successfully.
   - ``FAILED`` — terminated with an error.

.. class:: RunExecution

   One concrete execution of a :class:`RunRequest`. A single run request with
   ``rerun_count = N`` produces N ``RunExecution`` records, each with a different
   random seed.

   **Fields**

   - ``id`` — UUID primary key.
   - ``run_request`` — FK to the parent :class:`RunRequest`.
   - ``engine_run_id`` — UUID assigned by the simulation engine.
   - ``seed`` — random seed for this execution.
   - ``current_step`` — last reported simulation step (updated during execution).
   - ``status`` — see :class:`RunStatus`.
   - ``created_at``, ``started_at``, ``finished_at`` — lifecycle timestamps.

----

Transformations
---------------

.. class:: TransformationRequest

   A request to transform one or more input artefacts into output artefacts using a
   named method. Current methods include ``netpreview`` (renders a network image)
   and ``inspect`` (extracts network metadata).

   Transformations are executed by Celery workers and follow the same
   :class:`TransformStatus` lifecycle as run requests.

   **Fields**

   - ``id`` — UUID primary key.
   - ``method`` — the transformation to apply (e.g. ``"netpreview"``).
   - ``spec_snapshot`` — a snapshot of the transformation spec at request time.
   - ``parameters`` — method-specific parameters.
   - ``status`` — current state.
   - ``input_artefacts`` — M2M to :class:`Artefact` via :class:`TransformationInput`.
   - ``output_artefacts`` — M2M to :class:`Artefact` via :class:`TransformationOutput`.

.. class:: TransformationInput

   Through-model for ``TransformationRequest.input_artefacts``. Records which
   artefact was bound to which named input slot of the transformation.

.. class:: TransformationOutput

   Through-model for ``TransformationRequest.output_artefacts``. Records which
   artefact was produced in which named output role.

----

Logging
-------

.. class:: WorkerLogEntryRunRequest

   A structured log entry emitted by a worker while processing a :class:`RunRequest`.
   Includes level, message, and optional exception type and traceback.

.. class:: WorkerLogEntryRunExecution

   As above, but scoped to a :class:`RunExecution`.

.. class:: WorkerLogEntryTransformRequest

   As above, but scoped to a :class:`TransformationRequest`.

.. class:: SubscriptionLogEntry

   A timestamped record of a subscription payload received during a
   :class:`RunExecution`. Subscriptions are named data streams emitted by the
   simulation at each step (e.g. detector counts, queue lengths). Each entry
   records the simulation time, step number, subscription fingerprint, and the
   raw payload.

   Indexed on ``(run_execution, subscription_fingerprint)`` for efficient
   time-series retrieval.

.. class:: TelemetryLogEntry

   Similar to :class:`SubscriptionLogEntry`, but for named telemetry channels
   rather than subscriptions.

.. class:: RPCLogEntry

   Records individual RPC calls and responses exchanged between the Django layer
   and the simulation engine during a :class:`RunExecution`. Useful for debugging
   controller interactions.

----

Experiment Graphs
-----------------

.. class:: ExperimentGraph

   A saved visual pipeline graph associated with a :class:`Scenario` and optionally
   a compiled :class:`Experiment`. Graphs are authored in the browser-based graph
   builder and stored as JSON. Each save to an existing name increments the version.

   **Fields**

   - ``id`` — UUID primary key.
   - ``name`` — graph name (versioned).
   - ``version`` — auto-incremented per name.
   - ``scenario`` — the scenario this graph targets.
   - ``experiment`` — the compiled experiment derived from this graph, if any.
   - ``graph`` — the raw JSON graph definition.
