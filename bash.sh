#!/bin/bash
# creates a dir called package
mkdir package_for_lambda_ingestion

# install pg8000 into package dir
pip install pg8000 -t package_for_lambda_ingestion/
pip install pandas -t package_for_lambda_ingestion/
pip install dotenv -t package_for_lambda_ingestion/
pip install pyarrow -t package_for_lambda_ingestion/
pip install pyarrow.parquet -t package_for_lambda_ingestion/


# Define source and destination directories
SRC="src"
DEST="lambda_function_ingestion"

# Create the destination directory if it doesn't exist
mkdir -p "$DEST"

# Copy all files and directories except __pycache__
rsync -av --exclude='__pycache__' "$SRC"/ "$DEST"

echo "Copy completed successfully."
