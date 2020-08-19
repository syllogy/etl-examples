# Apache Beam Example Python

You will need:

[python 3.8](https://www.python.org/downloads/)

[pip: should be included in python install](https://pip.pypa.io/en/stable/installing/)

[poetry](https://pypi.org/project/poetry/)

A service account key in json form, something like:
```
{
  "type": "service_account",
  "project_id": "",
  "private_key_id": "",
  "private_key": "",
  "client_email": "",
  "client_id": "",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": ""
}
```

## Batch

```bash
export GOOGLE_APPLICATION_CREDENTIALS=<path to your key>

pip install poetry

poetry install

export REGION=europe-west1 <-- example

export PROJECT=david-playground-1 <-- example

export BUCKET=david-playground-ordered <-- example

poetry run python wordcount.py --region $REGION \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output gs://$BUCKET/results/outputs \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/
```

## Stream

```bash
export GOOGLE_APPLICATION_CREDENTIALS=<path to your key>

pip install poetry

poetry install

poetry run python streaming.py  \
  --region $REGION \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/ \
  --input_topic "projects/${PROJECT}/topics/YOUR_INPUT_TOPIC" \
  --output_topic "projects/${PROJECT}/topics/YOUR_OUTPUT_TOPIC" \
  --streaming
```
