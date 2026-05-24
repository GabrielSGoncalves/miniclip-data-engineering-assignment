import os
import sys

# Ensures Spark workers use the same Python as the test runner — required when multiple
# Python versions coexist on the machine (PySpark throws PYTHON_VERSION_MISMATCH otherwise).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
