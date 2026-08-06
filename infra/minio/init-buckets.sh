#!/bin/sh
set -e

echo "Waiting for MinIO to be ready..."
until (/usr/bin/mc alias set myminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}); do
    echo "MinIO is not ready yet. Retrying in 2 seconds..."
    sleep 2
done

echo "MinIO is ready! Creating buckets..."
/usr/bin/mc mb --ignore-existing myminio/landing
/usr/bin/mc mb --ignore-existing myminio/bronze
/usr/bin/mc mb --ignore-existing myminio/silver
/usr/bin/mc mb --ignore-existing myminio/gold

echo "Setting public download policy for development (optional)..."
/usr/bin/mc anonymous set download myminio/landing || true
/usr/bin/mc anonymous set download myminio/bronze || true
/usr/bin/mc anonymous set download myminio/silver || true
/usr/bin/mc anonymous set download myminio/gold || true

echo "MinIO Bucket initialization completed successfully!"
