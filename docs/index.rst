TrafficGym
==========

TrafficGym is a Django + Celery + SUMO platform for defining, running, and
analysing traffic simulation experiments.

Experiments are Python classes that subclass :class:`~trafficgym.engine.experiment.Experiment`
and implement a single ``run()`` method. The platform handles scheduling,
seeding, subscription logging, and analytics.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   db_reset
   case_study_ramp_meter
   case_study_generating_a_network
   case_study_file_based_experiment
   extending
   engine.experiment
   engine.ports
   engine.control
   engine.adapters
   engine.transformations
   models

Indices
-------

* :ref:`genindex`
* :ref:`modindex`
