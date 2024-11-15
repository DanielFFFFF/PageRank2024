import re
import sys
import time
from datetime import datetime
from operator import add
from typing import Iterable, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import storage
from pyspark.sql.functions import col, explode, lit, sum as spark_sum, size, hash


def computeContribs(urls: Iterable[str], rank: float) -> Iterable[Tuple[str, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls: str) -> Tuple[str, str]:
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]


if __name__ == "__main__":

    num_nodes = int(sys.argv[3])  # New parameter for the number of nodes
    # Define the bucket and path for the text file
    bucket_name = "pagerank_bucket_100"
    text_file_path = "times/elapsed_time_RDD_nodes=" + str(num_nodes) + ".txt"

    # Read input file and parse neighbours
    input_path = "gs://pagerank_bucket_100/small_page_links.nt"

    # Start timer & initialise Spark session
    start_time = time.time()

    # Initialize the spark context.

    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()



    # Chargement et parsing du fichier d'entrée
    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbors(urls)
                      ).distinct().groupByKey().cache()

    # Initialisation des rangs avec une valeur de 1.0
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    num_partitions = num_nodes
    # Boucle d'itération pour calculer PageRank
    for iteration in range(10):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(
            lambda rank: rank * 0.85 + 0.15)

    # Calculate elapsed time & Append as a row
    end_time = time.time()
    elapsed_time = end_time - start_time


    output_path = "gs://pagerank_bucket_100/RDD/output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ranks.saveAsTextFile(output_path)

    # Access the bucket
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    # Create a new blob (file) and upload the content
    blob = bucket.blob(text_file_path)
    content = f"Elapsed Time: {elapsed_time:.2f} seconds | Num Nodes: {num_nodes} | Method: RDD without URL partitioning\n"

    # Upload the new content back to the file
    blob.upload_from_string(content)

    # Finally, Stop Spark session
    spark.stop()