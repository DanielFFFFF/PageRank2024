import re
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum
from pyspark.sql import Row


def split_neighbours(line: str):
    """
    Parses a URL pair string into URLs pair.

    Args:
        line (str): A string containing a URL pair separated by whitespace.

    Returns:
        tuple: A tuple containing the two URLs.
    """
    parts = re.split(r'\\s+', line)
    return (parts[0], parts[2])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Start timer & Initialise Spark session
    start_time = time.time()
    spark = SparkSession.builder.appName("PythonPageRankDataFrame").getOrCreate()

    # Read input file and parse neighbours
    input_path = sys.argv[1]
    iterations = int(sys.argv[2])
    lines = spark.read.text(input_path)
    neighbours_df = lines.rdd.map(lambda row: split_neighbours(row[0])).toDF(["url", "neighbour"])

    # Initialise ranks with 1.0 for each URL
    ranks_df = neighbours_df.select("url").distinct().withColumn("rank", lit(1.0))

    # PageRank iterations
    for iteration in range(iterations):
        # Compute contributions from neighbours
        contribs_df = neighbours_df.join(ranks_df, neighbours_df.url == ranks_df.url) \
            .select(neighbours_df["neighbour"].alias("url"), (ranks_df["rank"] / 2).alias("contrib"))

        # Aggregate contributions and apply PageRank formula
        ranks_df = contribs_df.groupBy("url").agg(spark_sum("contrib").alias("rank"))
        ranks_df = ranks_df.withColumn("rank", ranks_df["rank"] * 0.85 + 0.15)

    # Calculate elapsed time & Append as a row
    end_time = time.time()
    elapsed_time = end_time - start_time
    end_time_row = spark.createDataFrame([Row(url="END_TIME", rank=lit(elapsed_time))])

    # Combine ranks with elapsed time and save to GCS
    output_df = ranks_df.unionByName(end_time_row)
    output_path = f"gs://$BUCKET_NAME/$OUTPUT_PATH"
    ranks_df.write.mode("overwrite").csv(output_path)

    # Finally, Stop Spark session
    spark.stop()