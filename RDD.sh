```
#!/bin/bash

# Télécharger les données PageRank
curl -o small_page_links.nt https://raw.githubusercontent.com/momo54/large_scale_data_management/main/small_page_links.nt

# Variables à personnaliser
PROJECT_ID="pagerank-441717"
BUCKET_NAME="pagerank_bucket_100"
CLUSTER_NAME="wordcount-cluster"
REGION="us-central1"
INPUT_FILE_PATH="small_page_links.nt"
OUTPUT_PATH="output"
ZONE="us-central1-a"
PAGERANK_SCRIPT="RDD.py"
ITERATIONS=10

# Activer les APIs nécessaires
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com

# Configurer le projet par défaut
gcloud config set project $PROJECT_ID

# Créer un bucket GCS (si nécessaire)
gsutil mb -l $REGION gs://$BUCKET_NAME/

# Copier le fichier d'entrée dans le bucket
gsutil cp $INPUT_FILE_PATH gs://$BUCKET_NAME/

# Créer un script PySpark PageRank avec un chronomètre
curl -o $PAGERANK_SCRIPT https://raw.githubusercontent.com/DanielFFFFF/PageRank2024/refs/heads/main/RDD.py


# Créer un cluster Dataproc minimal
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50GB \
    --image-version=2.0-debian10

# Soumettre le job PySpark au cluster
gcloud dataproc jobs submit pyspark $PAGERANK_SCRIPT \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    -- gs://$BUCKET_NAME/$INPUT_FILE_PATH $ITERATIONS

# Attendre la fin du job
sleep 10

# Lister les résultats dans GCS
gsutil ls gs://$BUCKET_NAME/$OUTPUT_PATH/

# Supprimer le cluster après l'exécution
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet

