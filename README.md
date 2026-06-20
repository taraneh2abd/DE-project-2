# Run

- docker compose up -d

- login to minio

- create bucket with name = output

- docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/revenue_analysis.py

#### for deleting the result's checkpoint and start again

- docker exec -it spark-master rm -rf /opt/spark-data/_checkpoints/revenue_analysis

![alt text](<Screenshot (956).png>)