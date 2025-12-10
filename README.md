# DRT Advance Passenger Information (API) import service

This service ingests Advance Passenger Information (API) data and provides a REST API for querying passenger information.

## Overview
This codebase contains a Scala backend built using sbt. The service fetches API data from an S3 bucket, parses it and
persists in a central database

## Scala Backend

NB this app requires the ACP prod VPN in order to access the S3 bucket 
To run the backend, enter the root of the codebase and run:

```bash
USE_PG_SSL=false \
USE_PG_SSL_MODE=disable \
AWS_ACCESS_KEY_ID=<secret> \
AWS_SECRET_ACCESS_KEY=<secret> \
BUCKET_NAME=drt-prod-extra-s3 \
NO_JSON_LOGGING= \
sbt -J-Duser.timezone=UTC run
```

Make sure to replace `<secret>` with your actual AWS credentials and bucket name from Kubernetes secrets.


You'll need to be connected to the ACP prod VPN to access secrets

`kubectl -n drt-preprod get secrets dq-s3-bucket-secret -o yaml`

AWS_ACCESS_KEY_ID is from dq-s3-bucket-secret -> access_key_id

AWS_SECRET_ACCESS_KEY is from dq-s3-bucket-secret -> secret_access_key

Once you have the secret strings from kubernetes you can decode them with

`echo -n base64stringhere | base64 -d`


Once you're able to run the app you should see data starting to populate 3 tables:
- voyage_manifest_passenger_info
- processed_json
- processed_zip

