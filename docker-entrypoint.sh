#!/bin/bash
set -e

# Locate the eclipse-sumo package directory so SUMO_HOME points to a
# directory containing a tools/ subdirectory, as expected by libsumo_adapter.
export SUMO_HOME=$(python3 -c "import sumo, os; print(os.path.dirname(sumo.__file__))")

exec "$@"
