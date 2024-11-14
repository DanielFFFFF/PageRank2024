import re
import sys
import time
from operator import add
from typing import Iterable, Tuple
from pyspark.resultiterable import ResultIterable
from pyspark.sql import SparkSession

def computeContribs(urls: Iterable[str], rank: float) -> Iterable[Tuple[str, float]]:
    """
    Calculates URL contributions to the rank of other URLs.

    Args:
        urls (Iterable[str]): URLs to which contributions are made.
        rank (float): Rank value to be distributed.

    Yields:
        tuple: URL and its corresponding contribution.
    """
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbours(urls: str) -> Tuple[str, str]:
    """
    Parses a URLs pair string into URLs pair.

    Args:
        urls (str): A string containing a URL pair separated by whitespace.

    Returns:
        tuple: Tuple containing the two URLs.
    """
    parts = re.split(r'\\s+', urls)
    return parts[0], parts[2]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Start timer & initialise Spark session
    start_time = time.time()
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()

    # Load input file and initialise links
    lines = spark.read.text("gs://pagerank_bucket_100/small_page_links.nt").rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbours(urls)).distinct().groupByKey().cache()

    # Initialise ranks for each URL
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # PageRank algorithm iterations
    for iteration in range(int(sys.argv[2])):
        # Compute contributions and update ranks
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]
        ))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Calculate and print elapsed time, and convert it to a Dataframe row
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total PageRank computation time: {elapsed_time:.2f} seconds")
    end_time_row = spark.createDataFrame([Row(url="END_TIME", rank=lit(elapsed_time))])

    # Save results and append elapsed time row
    output_path = "gs://pagerank_bucket_100/output"
    ranks.saveAsTextFile(output_path)

    # Stop Spark session
    spark.stop()