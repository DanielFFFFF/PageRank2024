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
ITERATIONS=10

# Enable necessary APIs
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com

# Configure the default project
gcloud config set project $PROJECT_ID

# Create a GCS bucket (if necessary)
gsutil mb -l $REGION gs://$BUCKET_NAME/ || echo "Bucket already exists."

# Download the PySpark PageRank script using DataFrames with a timer
rm DF.py
curl -o DF.py https://raw.githubusercontent.com/DanielFFFFF/PageRank2024/refs/heads/main/DF.py

# Copy the input file to the bucket
gsutil cp $INPUT_FILE_PATH gs://$BUCKET_NAME/

# Loop through all .py files in the current directory
for PYSCRIPT in *.py; do
    echo "Processing script: $PYSCRIPT"

    # Loop through 1 to 4 nodes configuration
    for WORKERS in 1 2 3 4; do
        echo "Creating cluster with $WORKERS worker node(s) for script $PYSCRIPT..."

        if [ $WORKERS -eq 1 ]; then
            # Create a single-node cluster (master node only, no workers)
            gcloud dataproc clusters create $CLUSTER_NAME \
                --region=$REGION \
                --zone=$ZONE \
                --single-node \
                --master-machine-type=n1-standard-2 \
                --master-boot-disk-size=50GB \
                --image-version=2.0-debian10
        else
            # Create a multi-node cluster with the specified number of workers
            gcloud dataproc clusters create $CLUSTER_NAME \
                --region=$REGION \
                --zone=$ZONE \
                --master-machine-type=n1-standard-2 \
                --master-boot-disk-size=50GB \
                --num-workers=$WORKERS \
                --worker-machine-type=n1-standard-2 \
                --worker-boot-disk-size=50GB \
                --image-version=2.0-debian10
        fi

        # Submit the PySpark job to the cluster, passing the number of nodes as a parameter
        echo "Submitting job to cluster with $WORKERS worker node(s) for script $PYSCRIPT..."
        gcloud dataproc jobs submit pyspark $PYSCRIPT \
            --cluster=$CLUSTER_NAME \
            --region=$REGION \
            -- gs://$BUCKET_NAME/$INPUT_FILE_PATH $ITERATIONS $WORKERS

        # Wait for the job to finish
        sleep 10

        # List results in GCS
        echo "Listing results for cluster with $WORKERS worker node(s) for script $PYSCRIPT..."
        gsutil ls gs://$BUCKET_NAME/$OUTPUT_PATH/

        # Delete the cluster after execution
        echo "Deleting cluster with $WORKERS worker node(s) for script $PYSCRIPT..."
        gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
    done
done

echo "All jobs completed for all .py scripts with configurations from 1 to 4 worker nodes."
