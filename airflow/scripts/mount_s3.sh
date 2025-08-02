#!/bin/bash

echo "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" > /etc/passwd-s3fs
chmod 600 /etc/passwd-s3fs

mkdir -p /mnt/datalake
s3fs datalake /mnt/datalake -o url=http://minio:9000 -o use_path_request_style -o allow_other

exec "$@"
echo "Bucket mounted at /mnt/datalake"
