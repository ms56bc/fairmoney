#!/bin/bash
# Check if the number of arguments is correct
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <job_jar_name>"
    exit 1
fi

# Extract the last item from the path
FLINK_JOB_JAR=$1

# Flink Job arguments
#FLINK_JOB_ARGS="--inputPath input.txt --outputPath output"

# Flink JobManager address
FLINK_JOBMANAGER_ADDRESS="http://localhost:8081"

# Step 1: Upload the Flink JAR file
upload_response=$(curl -X POST -H "Expect:" -H "Content-Type: multipart/form-data" \
  -F "jarfile=@$FLINK_JOB_JAR" \
  $FLINK_JOBMANAGER_ADDRESS/jars/upload)

# Extract the uploaded JAR ID from the response
jar_id=$(echo "$upload_response" | jq -r '.filename' | cut -d '/' -f 5)
echo "$upload_response"
echo "Uploaded JAR with ID: $jar_id"
# Step 2: Submit the Flink job using the uploaded JAR ID
submit_response=$(curl -X POST -H "Content-Type: application/json" -d '{
  "programArgs": "'$FLINK_JOB_ARGS'",
  "parallelism": 1
}' $FLINK_JOBMANAGER_ADDRESS/jars/$jar_id/run)
echo "$submit_response"
# Extract the Job ID from the submit response
job_id=$(echo "$submit_response" | jq -r '.jobid')

echo "Flink job submitted with Job ID: $job_id"

