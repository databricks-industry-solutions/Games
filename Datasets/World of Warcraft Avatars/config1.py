# Databricks notebook source
# MAGIC %run "../../config/environment_config"

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

import re
import os
import json
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

from pyspark.sql import functions as F

# COMMAND ----------

database_name = f"World_Of_Warcraft_Avatars"
database_location = f"{tmpdir}games"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

# COMMAND ----------

print(f"Database Name: {database_name}")
