#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tabulate import tabulate

from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer


spark = (
    SparkSession.builder.master("local[*]")
    .appName("Point Construction, Transform, and Distance Test")
    .config("spark.serializer", KryoSerializer.getName)
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-python-adapter-2.4_2.11:1.0.0-incubating,"
        "org.datasyslab:geotools-wrapper:geotools-24.0",
    )
    .getOrCreate()
)

SedonaRegistrator.registerAll(spark)

temp = spark.sql(
"""
select
ST_Point(-117.105397, 33.17972) as p1,
ST_Transform(ST_Point(-117.105397, 33.17972), 'epsg:4326', 'epsg:5071', false) as p1t,
ST_Transform(ST_FlipCoordinates(ST_Point(-117.105397, 33.17972)), 'epsg:4326', 'epsg:5071', false) as p1_flip_t
"""
).toPandas()
print(tabulate(temp, headers=temp.columns, tablefmt="github", showindex=False))

#  temp = spark.sql(
#  """
#  select
#  ST_Point(-117.105397, 33.17972) as p1,
#  ST_Point(-117.089177, 33.186309) as p2,
#  ST_Transform(ST_Point(-117.105397, 33.17972), 'epsg:4326', 'epsg:5071', false) as p1t,
#  ST_Transform(ST_Point(-117.089177, 33.186309), 'epsg:4326', 'epsg:5071', false) as p2t,
#  ST_Distance(ST_Point(-117.105397, 33.17972), ST_Point(-117.089177, 33.186309)) as orig_p_distance,
#  ST_Distance(ST_POINT(-8385001.333595577, -4229176.29059952), ST_POINT (-8385712.402749769, -4228138.150187418)) as t_distance,
#  ST_Distance(ST_Transform(ST_FlipCoordinates(ST_Point(-117.105397, 33.17972)), 'epsg:4326', 'epsg:5071', false), ST_Transform(ST_FlipCoordinates(ST_Point(-117.089177, 33.186309)), 'epsg:4326', 'epsg:5071', false)) as t_flip_coordinates
#  """
#  ).toPandas()
#  print(tabulate(temp, headers=temp.columns, tablefmt="github", showindex=False))
