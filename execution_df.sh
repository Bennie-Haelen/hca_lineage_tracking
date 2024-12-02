#!/bin/python

# python /Users/rahul.vangala/sada_projects/HCA/hca_lineage_tracking/dataflow_lineage/Dataflow.py \
#     --worker_zone=us-central1-a \
#     --no_use_public_ips \
#     --experiments=use_network_tags=dataflow

# gcloud dataflow jobs run word_count_test \
#     --gcs-location gs://dataflow-templates/latest/Word_Count \
#     --region us-central1 \
#     --parameters \
#     inputFile=gs://dataflow-samples/shakespeare/kinglear.txt,output=gs://bennie_bucket_for_dataflow/temp/testoutput


# python /Users/rahul.vangala/sada_projects/HCA/hca_lineage_tracking/dataflow_lineage/dataflow_gcs.py \
#     --worker_zone=us-central1-a \
#     --no_use_public_ips \
#     --experiments=use_network_tags=dataflow

python /Users/rahul.vangala/sada_projects/HCA/hca_lineage_tracking/dataflow_lineage/dataflow_gcs_2.py \
    --worker_zone=us-central1-a \
    --no_use_public_ips \
    --experiments=use_network_tags=dataflow \
    --service_account="lineage-sand@hca-sandbox.iam.gserviceaccount.com"


python /Users/rahul.vangala/sada_projects/HCA/hca_lineage_tracking/dataflow_lineage/dataflow_gcs_3.py \
    --worker_zone=us-central1-a \
    --no_use_public_ips \
    --experiments=use_network_tags=dataflow \
    --service_account="lineage-sand@hca-sandbox.iam.gserviceaccount.com"