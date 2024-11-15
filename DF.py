import re
import sys
import time
from pyspark.sql import SparkSession
from typing import Tuple
from pyspark.sql.functions import col, explode, lit, sum as spark_sum, size, hash
from pyspark.sql import functions as F
from google.cloud import storage


def parse_neighbors(line: str) -> Tuple[str, str]:
    """Parse une ligne pour extraire une paire d'URLs."""
    parts = re.split(r'\s+', line)
    return parts[0], parts[2]

if __name__ == "__main__":
    num_nodes = int(sys.argv[3])  # New parameter for the number of nodes

    spark = SparkSession.builder.appName("PythonPageRankDataFrame").getOrCreate()

    # Define the bucket and path for the text file
    bucket_name = "pagerank_bucket_100"
    text_file_path = "times/elapsed_time_DF_nodes=" + str(num_nodes) + ".txt"

    # Read input file and parse neighbours
    input_path = "gs://pagerank_bucket_100/small_page_links.nt"
    iterations = 10
    lines = spark.read.text(input_path)
    neighbours_df = lines.rdd.map(lambda row: parse_neighbors(row[0])).toDF(["url", "neighbour"])
    links = lines.rdd.map(lambda row: parse_neighbors(row.value)).toDF(["src", "dst"])


    # Grouper par source pour créer une liste des liens sortants
    links = links.groupBy("src").agg(F.collect_list("dst").alias("links"))

    # Initialiser les rangs avec une valeur de 1.0 pour chaque URL
    ranks = links.select("src").withColumn("rank", lit(1.0))


    # Start timer & Initialise Spark session
    start_time = time.time()
    # PageRank iterations
    for iteration in range(iterations):
        # Calcul des contributions de chaque lien
        contribs = links.alias("l").join(ranks.alias("r"), col("l.src") == col("r.src")) \
            .select(
            col("l.src").alias("src"),
            explode(col("l.links")).alias("dst"),
            (col("r.rank") / size(col("l.links"))).alias("contrib")
        )

        # Calcul des nouveaux rangs par agrégation des contributions
        ranks = contribs.groupBy("dst").agg(spark_sum("contrib").alias("rank"))

        # Appliquer le facteur de décroissance de PageRank
        ranks = ranks.withColumn("rank", col("rank") * 0.85 + 0.15)

        # Renommer `dst` en `src` pour l'itération suivante
        ranks = ranks.withColumnRenamed("dst", "src")

    # Calculate elapsed time & Append as a row
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Combine ranks with elapsed time and save to GCS
    output_path = "gs://pagerank_bucket_100/output"
    ranks.write.mode("overwrite").csv(output_path)

    # Access the bucket
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    # Create a new blob (file) and upload the content
    blob = bucket.blob(text_file_path)
    content = f"Elapsed Time: {elapsed_time:.2f} seconds | Num Nodes: {num_nodes} | Method: DF without URL partitioning\n"

    # Upload the new content back to the file
    blob.upload_from_string(content)

    # Finally, Stop Spark session
    spark.stop()