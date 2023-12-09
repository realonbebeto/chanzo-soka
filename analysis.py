from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from db_config import config
import pandas as pd
from pyspark.sql.types import FloatType
import math

conf = (
    SparkConf()
    .setAppName("ETL Source Football")
    .set(
        "spark.jars",
        "/Users/realonbebeto/Kazispace/local/py/scrftbl/postgresql-42.7.1.jar",
    )
)
sc = SparkContext(conf=conf)
session = SparkSession(sc)

jdbc_url = "jdbc:postgresql://localhost:5432/main"
connection_properties = {
    "user": config.db_username,
    "password": config.db_password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}


# ===========================
# Common
# ============================
def rangify(width: int):
    """
    A function that takes in integer second wiidth and returns a list of tuples with classes
    """
    num_classes = int((9000 - 0) / width) + 1
    return [(i * width, (i + 1) * width - 1) for i in range(num_classes)]


def minutefy(val):
    """
    A function that takes a range in string format and converts the bounds from seconds to minutes
    """
    lower, upper = val.split("-")
    lower = round(int(lower) / 60)
    upper = round(int(upper) / 60)
    return f"{lower}-{upper}"


# ===========================
# Class Ranges
# ============================

class_ranges_i = rangify(300)
class_ranges_s = rangify(120)

# Read the the spatial fact
df = session.read.jdbc(
    jdbc_url, "analytics.spatial_fact", properties=connection_properties
)

# Sorting the spark dataframe
df = df.sort("timestamp", ascending=[True])

# ===========================
# Intense Calculations
# ============================
# Define conditions for each intense class
conditions = [
    (f"timestamp >= {lower} AND timestamp < {upper+1}", f"{lower}-{upper}")
    for lower, upper in class_ranges_i
]

# Create a new column 'intense_class' based on conditions
action_df = df.withColumn(
    "intense_class",
    f.expr(
        "CASE "
        + " ".join(f"WHEN {cond} THEN '{label}'" for cond, label in conditions)
        + " END"
    ),
)

w0 = Window.partitionBy(["match_id", "intense_class"])
action_df = action_df.withColumn(
    "locations", f.concat(f.col("x").cast("string"), f.col("y").cast("string"))
)


action_df = action_df.withColumn("actions", f.count("locations").over(w0))


action_df = (
    action_df.select(["intense_class", "actions"])
    .distinct()
    .sort("actions", ascending=[False])
    .toPandas()
)


action_df["intense_class"] = action_df["intense_class"].apply(lambda x: minutefy(x))

action_df.to_csv("actions.csv", index=False)


# ===========================
# Spread Calculations
# ============================
# Define conditions for each spread class
conditions = [
    (f"timestamp >= {lower} AND timestamp < {upper+1}", f"{lower}-{upper}")
    for lower, upper in class_ranges_s
]

# Create a new column 'spread_class' based on conditions
df = df.withColumn(
    "spread_class",
    f.expr(
        "CASE "
        + " ".join(f"WHEN {cond} THEN '{label}'" for cond, label in conditions)
        + " END"
    ),
)

w1 = Window.partitionBy(["match_id", "spread_class", "trackable_object"])
spread_df = df.filter(f.col("trackable_object") != 55).withColumns(
    {"avg_min_x": f.avg("x").over(w1), "avg_min_y": f.avg("y").over(w1)}
)

w2 = Window.partitionBy(["match_id", "spread_class"])

spread_df = (
    spread_df.select(
        [
            "match_id",
            "trackable_object",
            "period",
            "spread_class",
            "avg_min_x",
            "avg_min_y",
        ]
    )
    .distinct()
    .withColumns(
        {
            "all_avg_x": f.collect_list("avg_min_x").over(w2),
            "all_avg_y": f.collect_list("avg_min_y").over(w2),
        }
    )
)


# Function to calculate spread metric
def calculate_spread(x, y):
    num_loc = len(x)
    spread = 0

    for i in range(num_loc):
        for j in range(i + 1, num_loc):
            # Calculate the Euclidean distance between location i and location j
            distance = math.sqrt((x[j] - x[i]) ** 2 + (y[j] - y[i]) ** 2)
            spread += distance

    return spread


spread_udf = f.udf(calculate_spread, FloatType())

spread_df = (
    spread_df.withColumn("spread", spread_udf("all_avg_x", "all_avg_y"))
    .select(["spread_class", "spread"])
    .sort("spread", ascending=[False])
    .toPandas()
)

spread_df = spread_df.drop_duplicates()

spread_df["spread_class"] = spread_df["spread_class"].apply(lambda x: minutefy(x))

spread_df.to_csv("spread.csv", index=False)
