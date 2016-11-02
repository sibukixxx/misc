#1/bin/bash

curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init
