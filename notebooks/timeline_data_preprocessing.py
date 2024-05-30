# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: PySpark
#     language: python
#     name: pysparkkernel
# ---

# # Final - Robust Journey Planning
# ---
#
# Imagine you are a regular user of the public transport system, and you are checking the operator's schedule to meet your friends for a class reunion. The choices are:
#
# 1. You could leave in 10mins, and arrive with enough time to spare for gossips before the reunion starts.
#
# 2. You could leave now on a different route and arrive just in time for the reunion.
#
# Undoubtedly, if this is the only information available, most of us will opt for option 1.
#
# If we now tell you that option 1 carries a fifty percent chance of missing a connection and be late for the reunion. Whereas, option 2 is almost guaranteed to take you there on time. Would you still consider option 1?
#
# Probably not. However, most public transport applications will insist on the first option. This is because they are programmed to plan routes that offer the shortest travel times, without considering the risk factors.

# ---
# ⚠️ **Note**: all the data used in this homework is described in the [FINAL-PREVIEW](../final-preview.md) document, which can be found in this repository. The document describes the final project due for the end of this semester.
#
# For this notebook you are free to use the following tables, which can all be found under the `com490` database.
# - You can list the tables with the command `SHOW TABLES IN com490`.
# - You can see the details of each table with the command `DESCRIBE EXTENDED com490.{table_name}`.
#
# * com490.sbb_orc_calendar
# * com490.sbb_orc_stop_times
# * com490.sbb_orc_stops
# * com490.sbb_orc_trips
# * com490.geo_shapes
#
# They are all part of the public transport timetable that was published on the week of 10.1.2024.
#
# ---
# For your convenience we also define useful python variables:
#
# * default_db=`com490`
#     * The Hive database shared by the class, do not drop or modify the content of this database.
# * hadoop_fs=`hdfs://iccluster067.iccluster.epfl.ch:8020`
#     * The HDFS server, in case you need it for hdfs commands with hive, pandas or pyarrow
# * username:
#     * Your user id (EPFL gaspar id), use it as your default database name.

# ---

# +
import os
import numpy as np
import plotly.express as px
import heapq
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, unix_timestamp, to_timestamp, sqrt, pow, lit, radians, sin, cos, asin
# -

# ---
# ## Start a spark Session environment

# #### Session information (%%info)
#
# Livy is an open source REST server for Spark. When you execute a code cell in a PySpark notebook, it creates a Livy session to execute your code. You can use the %%info magic to display the current Livy sessions information, including sessions of others.

# %%info

print(f'Start Spark name:{spark._sc.appName}, version:{spark.version}')

# The new sessions is listed if you use the %%info magic function again

# %%info

# A SparkSession object `spark` is initialized.

type(spark)

# Be nice to others - remember to add a cell `spark.stop()` at the end of your notebook.

# You can confirm this by executing a command that displays the value of the USER environment variable. Start by running it on the remote cluster (without using %%local), and then check it in the local environment as well.

import os
print(f"remote USER={os.getenv('USER',None)}")

# %%local
import os
print(f"local USER={os.getenv('USER',None)}")

# Below, we provide the `username` and `hadoop_fs` as Python variables accessible in both environments. You can use them to enhance the portability of your code, as demonstrated in the following Spark SQL command. Additionally, it's worth noting that you can execute SQL commands on Hive directly from Spark.

# %%local
import os
username=os.getenv('USER', 'anonymous')
hadoop_fs=os.getenv('HADOOP_DEFAULT_FS', 'hdfs://iccluster067.iccluster.epfl.ch:8020')
print(f"local username={username}\nhadoop_fs={hadoop_fs}")
 # (prevent deprecated np.bool error since numpy 1.24, until a new version of pandas/Spark fixes this)

# +
import numpy as np
np.bool = np.bool_

username=spark.conf.get('spark.executorEnv.USERNAME', 'anonymous')
hadoop_fs=spark.conf.get('spark.executorEnv.HADOOP_DEFAULT_FS','hdfs://iccluster067.iccluster.epfl.ch:8020')
print(f"remote username={username}\nhadoop_fs={hadoop_fs}")
# -

spark.sql(f'CREATE DATABASE IF NOT EXISTS {username}')
spark.sql(f'SHOW TABLES IN {username}').show(truncate=False)

spark.sql(f'SHOW TABLES IN {username}').toPandas()

# 💡 You can convert spark DataFrames to pandas DataFrame to process the results. Only do this for small result sets, otherwise your spark driver will run OOM.

# # we first begin by loading our data

stopsDf = spark.read.csv("/data/sbb/csv/timetables/stops/year=2024/month=04/day=29/stops.txt", sep=',', header=True, inferSchema=True)
stop_timesDf = spark.read.csv("/data/sbb/csv/timetables/stop_times/year=2024/month=04/day=29/stop_times.txt", sep=',', header=True, inferSchema=True)
tripsDf = spark.read.csv("/data/sbb/csv/timetables/trips/year=2024/month=04/day=29/trips.txt", sep=',', header=True, inferSchema=True)
calendarDf = spark.read.csv("/data/sbb/csv/timetables/calendar/year=2024/month=04/day=29/calendar.txt", sep=',', header=True, inferSchema=True)
routesDf = spark.read.csv("/data/sbb/csv/timetables/routes/year=2024/month=04/day=29/routes.txt", sep=',', header=True, inferSchema=True)

# # next we filter our datset to only get Lausanne area and trips of the weekday

# +
# Define bounding box for Lausanne
lat_min, lat_max = 46.5, 46.6
lon_min, lon_max = 6.5, 6.7

# Filter stops based on latitude and longitude
lausanne_stopsDf = stopsDf.filter((stopsDf['STOP_LAT'] >= lat_min) & 
                                  (stopsDf['STOP_LAT'] <= lat_max) &
                                  (stopsDf['STOP_LON'] >= lon_min) & 
                                  (stopsDf['STOP_LON'] <= lon_max)).select('STOP_ID', 'STOP_NAME', 'STOP_LAT', 'STOP_LON')

lausanne_stop_ids = lausanne_stopsDf.select('STOP_ID')

# Filter stop_times based on filtered stops
lausanne_stop_timesDf = stop_timesDf.join(lausanne_stop_ids, on='STOP_ID', how='inner')

# Filter trips based on filtered stop_times
lausanne_trip_ids = lausanne_stop_timesDf.select('TRIP_ID').distinct()
lausanne_tripsDf = tripsDf.join(lausanne_trip_ids, on='TRIP_ID', how='inner')

# Filter calendar based on filtered trips
lausanne_service_ids = lausanne_tripsDf.select('SERVICE_ID').distinct()
lausanne_calendarDf = calendarDf.join(lausanne_service_ids, on='SERVICE_ID', how='inner')

# Filter routes based on filtered trips
lausanne_route_ids = lausanne_tripsDf.select('ROUTE_ID').distinct()
lausanne_routesDf = routesDf.join(lausanne_route_ids, on='ROUTE_ID', how='inner')

# -
lausanne_stopsDf.show()

lausanne_stop_timesDf.show()

lausanne_tripsDf.show()

lausanne_calendarDf.show()

lausanne_routesDf.show()

# # next we'll calculate the mean travel time between every station connected by a trip

# +
spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")

# Join lausanne_stop_timesDf with lausanne_tripsDf to include route_id
lausanne_stop_timesDf = lausanne_stop_timesDf.join(lausanne_tripsDf.select('TRIP_ID', 'ROUTE_ID'), on='TRIP_ID')

# Convert departure times to timestamp
lausanne_stop_timesDf = lausanne_stop_timesDf.withColumn('DEPARTURE_TIME', to_timestamp('DEPARTURE_TIME', 'HH:mm:ss'))

# Self-join to get all pairs of stops within the same trip and route
lausanne_stop_pairsDf = lausanne_stop_timesDf.alias('df1').join(
    lausanne_stop_timesDf.alias('df2'),
    (col('df1.TRIP_ID') == col('df2.TRIP_ID')) & 
    (col('df1.ROUTE_ID') == col('df2.ROUTE_ID')) & 
    (col('df1.DEPARTURE_TIME') < col('df2.DEPARTURE_TIME')),
    how='inner'
).select(
    col('df1.STOP_ID').alias('STOP_ID_1'),
    col('df2.STOP_ID').alias('STOP_ID_2'),
    col('df1.ROUTE_ID').alias('ROUTE'),
    (unix_timestamp(col('df2.DEPARTURE_TIME')) - unix_timestamp(col('df1.DEPARTURE_TIME'))).alias('TRAVEL_TIME')
)

# Calculate mean travel time for each pair of stops
mean_travel_timesDf = lausanne_stop_pairsDf.groupBy('STOP_ID_1', 'STOP_ID_2', 'ROUTE').agg(avg('TRAVEL_TIME').alias('MEAN_TRAVEL_TIME'))

# Compute distances between stops using the Haversine formula
def haversine(lat1, lon1, lat2, lon2):
    return 2 * 6371 * asin(sqrt(
        pow(sin((radians(lat2) - radians(lat1)) / 2), 2) +
        cos(radians(lat1)) * cos(radians(lat2)) *
        pow(sin((radians(lon2) - radians(lon1)) / 2), 2)
    ))

lausanne_stops_distanceDf = lausanne_stopsDf.alias('df1').crossJoin(
    lausanne_stopsDf.alias('df2')
).select(
    col('df1.STOP_ID').alias('STOP_ID_1'),
    col('df2.STOP_ID').alias('STOP_ID_2'),
    col('df1.STOP_NAME').alias('STOP_NAME_1'),
    col('df2.STOP_NAME').alias('STOP_NAME_2'),
    haversine(
        col('df1.STOP_LAT'), col('df1.STOP_LON'),
        col('df2.STOP_LAT'), col('df2.STOP_LON')
    ).alias('DISTANCE')
)

# Filter pairs of stops within 500m and not the same stop
walking_distance_stopsDf = lausanne_stops_distanceDf.filter((col('DISTANCE') <= 0.5) & (col('DISTANCE') > 0))

# Calculate walking time assuming 50m per minute (1.2 m per second)
walking_distance_stopsDf = walking_distance_stopsDf.withColumn('MEAN_TRAVEL_TIME', (col('DISTANCE') * 1000 / 1.2)).withColumn('ROUTE', lit('WALKING'))

# Select required columns and ensure same format
walking_distance_stopsDf = walking_distance_stopsDf.select(
    col('STOP_ID_1').cast("string"), 
    col('STOP_ID_2').cast("string"), 
    col('STOP_NAME_1').cast("string"), 
    col('STOP_NAME_2').cast("string"), 
    col('MEAN_TRAVEL_TIME').cast("double"), 
    col('ROUTE').cast("string")
)

# Select required columns and ensure same format for mean_travel_timesDf
mean_travel_timesDf = mean_travel_timesDf.join(lausanne_stopsDf.withColumnRenamed('STOP_NAME', 'STOP_NAME_1'), on=mean_travel_timesDf['STOP_ID_1'] == lausanne_stopsDf['STOP_ID']) \
    .join(lausanne_stopsDf.withColumnRenamed('STOP_NAME', 'STOP_NAME_2'), on=mean_travel_timesDf['STOP_ID_2'] == lausanne_stopsDf['STOP_ID']) \
    .select(
        col('STOP_ID_1').cast("string"), 
        col('STOP_ID_2').cast("string"), 
        col('STOP_NAME_1').cast("string"), 
        col('STOP_NAME_2').cast("string"), 
        col('MEAN_TRAVEL_TIME').cast("double"), 
        col('ROUTE').cast("string")
    )

# Union the two DataFrames
final_travel_timesDf = mean_travel_timesDf.union(walking_distance_stopsDf)

# Show the final DataFrame
final_travel_timesDf.show()

# +
# save it the hdfs 
# save the df to hdfs 
username=spark.conf.get('spark.executorEnv.USERNAME', 'anonymous')
hadoop_fs=spark.conf.get('spark.executorEnv.HADOOP_DEFAULT_FS','hdfs://iccluster067.iccluster.epfl.ch:8020')
print(f"remote username={username}\nhadoop_fs={hadoop_fs}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {username}")
spark.sql(f"USE {username}")
table_name = "graph_data"
final_travel_timesDf.write.mode("overwrite").saveAsTable(f"{username}.{table_name}")
spark.sql(f"SHOW TABLES IN {username}").show(truncate=False)
# -


