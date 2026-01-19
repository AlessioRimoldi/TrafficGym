# TrafficGym
Traffic control interface based on SUMO

sumo_engine/
  core/
    session.py              # SimulationSession, lifecycle, config
    kernel.py               # libsumo wrapper, stepping, state acquisition
    snapshot.py             # StateSnapshot, typed views
    subscriptions.py        # what to fetch each tick (performance control)
    events.py               # Event bus and event types
    artifacts.py            # artifact store, provenance
    registry.py             # plugin registry, entry points

  actuators/
    manager.py              # ActuationManager, ActionBundle routing
    capabilities.py         # capability interfaces + schemas
    adapters/
      tls.py                # TrafficLightAdapter -> libsumo.trafficlight.*
      lane.py               # LaneAdapter -> libsumo.lane.*
      edge.py
      vehicle.py
      generic_setvar.py     # if you want a generic "set parameter" style
    schemas.py              # canonical action schemas

  pipelines/
    graph.py                # DAG execution engine
    node.py                 # base Node, typed ports
    builtins/
      window.py
      aggregate.py
      features.py
      logging.py
      metrics.py
      export_parquet.py
      control_loop.py       # observe->policy->actuate (just another node)

  editing/
    models/
      net.py                # network domain model
      demand.py             # routes/flows/trips
    xml/
      net_parser.py
      demand_parser.py
      writers.py
    tools/
      runner.py             # SUMO tools runner + provenance
      netconvert.py
      duarouter.py
      randomtrips.py

  grpc/
    service.py              # gRPC server implementation (thin orchestration)
    streaming.py            # telemetry streams
    mappers.py              # map internal objects -> protobuf messages
    
  storage/
    db/
      models.py          # SQLAlchemy models for experiments/runs/artifacts
      migrations/        # Alembic migrations
      repo.py            # repository layer (CRUD)
    artifact_store/
      store.py           # content-addressed store (local or S3)
      hashing.py
    telemetry/
      writer.py          # streaming writer: buffers and writes Parquet chunks
      schema.py          # canonical schemas for streams
      reader.py          # load slices for UI/plots

  cli/
    main.py                 # optional: run session from CLI for batch

  plugins/                  # optional: built-in plugins shipped with engine
