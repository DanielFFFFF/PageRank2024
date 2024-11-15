#!/bin/bash

# Download the PageRank data
curl -o small_page_links.nt https://raw.githubusercontent.com/momo54/large_scale_data_management/main/small_page_links.nt

# Variables to personalize
PROJECT_ID="pagerank-441717"
BUCKET_NAME="pagerank_bucket_100"
CLUSTER_NAME="wordcount-cluster"
REGION="us-central1"
INPUT_FILE_PATH="small_page_links.nt"
OUTPUT_PATH="output"
ZONE="us-central1-a"
PAGERANK_SCRIPT="DF.py"
ITERATIONS=10

# Enable necessary APIs
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com

# Configure the default project
gcloud config set project $PROJECT_ID

# Create a GCS bucket (if necessary)
gsutil mb -l $REGION gs://$BUCKET_NAME/

# Copy the input file to the bucket
gsutil cp $INPUT_FILE_PATH gs://$BUCKET_NAME/

# Delete pagerank before downloading it
rm -f $PAGERANK_SCRIPT
# Download the PySpark PageRank script using DataFrames with a timer
curl -o $PAGERANK_SCRIPT https://raw.githubusercontent.com/DanielFFFFF/PageRank2024/refs/heads/main/DF.py

# Create a 4 node cluster
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50GB \
    --num-workers=1 \
    --worker-machine-type=n1-standard-2 \
    --worker-boot-disk-size=50GB \
    --image-version=2.0-debian10


# Submit the PySpark job to the cluster
gcloud dataproc jobs submit pyspark $PAGERANK_SCRIPT \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    -- gs://$BUCKET_NAME/$INPUT_FILE_PATH $ITERATIONS

# Wait for the job to finish
sleep 10

# List results in GCS
gsutil ls gs://$BUCKET_NAME/$OUTPUT_PATH/

# Delete the cluster after execution
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet

