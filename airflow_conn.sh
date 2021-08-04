#!/bin/bash
# Run this script after setting up the Airflow containers

USER_NAME="ismaildawoodjee"
REGION="ap-southeast-1"
PROJECT_NAME="great-expectations-with-airflow"
BUCKET_NAME="greatex-bucket"

AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile "$USER_NAME")
AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile "$USER_NAME")

# Add S3 bucket name as a Variable to Airflow
sudo docker exec -d "$PROJECT_NAME"_airflow-webserver_1 \
        airflow variables set BUCKET_NAME "$BUCKET_NAME"

# Add AWS credentials as a Connection in Airflow
sudo docker exec -d "$PROJECT_NAME"_airflow-webserver_1 \
        airflow connections add 'aws_default' \
        --conn-type 'aws' \
        --conn-login "$AWS_ACCESS_KEY_ID" \
        --conn-password "$AWS_SECRET_ACCESS_KEY" \
        --conn-extra "{\"region_name\": \"$REGION\"}"
