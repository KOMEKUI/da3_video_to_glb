export POSTGRES_DSN="host=160.251.139.18 port=15432 dbname=app_db user=app_user password=pass1542"
export MINIO_ENDPOINT="160.251.139.18:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="change-this-strong-password"
export MINIO_SECURE="false"

export JOB_INPUT_BUCKET="inputs"
export JOB_OUTPUT_BUCKET="outputs"

export WORKER_KEY="$(hostname)"
export WORKER_DISPLAY_NAME="DA3 Worker - $(hostname)"
export WORKER_TAGS_JSON='{"gpu":"V100","runtime":"pytorch"}'
export WORKER_CAPACITY_JSON='{"gpus":1}'

export IDLE_SLEEP_SEC="2"
export KEEP_FRAMES_FOR_DEBUG="false"

export HF_TOKEN="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"

python -m app.worker_main