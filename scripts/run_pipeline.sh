#!/bin/bash
# Orquestra a execução do pipeline no LocalStack: roda o Glue job,
# espera terminar e depois dispara o crawler para catalogar a camada refined.
set -euo pipefail

ENDPOINT="http://localhost:4566"
JOB_NAME="etl-netflix"
CRAWLER_NAME="crawler-netflix"

echo ">> Disparando job Glue: ${JOB_NAME}"
RUN_ID=$(awslocal --endpoint-url "${ENDPOINT}" glue start-job-run --job-name "${JOB_NAME}" --query 'JobRunId' --output text)
echo "   JobRunId=${RUN_ID}"

echo ">> Aguardando conclusão do job..."
while true; do
  STATE=$(awslocal --endpoint-url "${ENDPOINT}" glue get-job-run --job-name "${JOB_NAME}" --run-id "${RUN_ID}" --query 'JobRun.JobRunState' --output text)
  echo "   estado atual: ${STATE}"
  case "${STATE}" in
    SUCCEEDED) break ;;
    FAILED|ERROR|TIMEOUT) echo "Job falhou com estado ${STATE}"; exit 1 ;;
  esac
  sleep 5
done

echo ">> Disparando crawler: ${CRAWLER_NAME}"
awslocal --endpoint-url "${ENDPOINT}" glue start-crawler --name "${CRAWLER_NAME}"

echo ">> Pipeline concluído. Consulte os dados no S3 (refined/netflix_titles) ou no RDS (public.netflix_titles)."
