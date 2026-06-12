Resetting the Database
======================

To reset the database in a Docker Compose environment, bring down all services,
restart the database container in isolation, drop and recreate the database, then
bring the full stack back up and run migrations.

.. code-block:: bash

   docker compose down
   docker compose up -d db

Wait for the database container to become healthy, then::

   docker compose exec db dropdb -U trafficgym trafficgym
   docker compose exec db createdb -U trafficgym trafficgym
   docker compose up -d
   docker compose exec web python manage.py migrate

.. warning::

   This permanently destroys all data in the database. There is no undo.

.. note::

   The database must be the only running service when ``dropdb`` is called.
   If other containers (web, worker) are connected to it, the drop will fail
   with a "being accessed by other users" error — hence the isolated
   ``docker compose up -d db`` step.