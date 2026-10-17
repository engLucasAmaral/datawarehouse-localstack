#!/usr/bin/env bash
# Prepara o S3 e registra o job Netflix usando o RDS criado no script 00.
set -euo pipefail

BUCKET="${DATA_BUCKET:-dw-datalake}"
DATABASE="${GLUE_DATABASE:-dw_analytics}"
ROLE_NAME="glue-local-role"
JOB_NAME="etl-netflix"
DW_JOB_NAME="etl-netflix-dw"
CRAWLER_NAME="crawler-netflix"
RDS_INSTANCE_ID="${RDS_INSTANCE_ID:-netflix-postgres}"
RDS_DB_NAME="${RDS_DB_NAME:-db_dadosbruto}"
RDS_MASTER_USERNAME="${RDS_MASTER_USERNAME:-dwadmin}"
RDS_MASTER_PASSWORD="${RDS_MASTER_PASSWORD:-dwlocal123}"

RAW_PATH="s3://${BUCKET}/raw/netflix-movies-and-series/"
OUTPUT_PATH="s3://${BUCKET}/refined/netflix_titles/"

echo ">> Preparando bucket s3://${BUCKET}"
awslocal s3 mb "s3://${BUCKET}" 2>/dev/null || true
for prefix in raw raw/netflix-movies-and-series refined refined/netflix_titles scripts; do
  awslocal s3api put-object --bucket "${BUCKET}" --key "${prefix}/" >/dev/null
done

echo ">> Publicando netflix_job.py"
awslocal s3 cp /opt/glue_jobs/netflix_job.py "s3://${BUCKET}/scripts/netflix_job.py"
echo ">> Publicando netflix_dw.py"
awslocal s3 cp /opt/glue_jobs/netflix_dw.py "s3://${BUCKET}/scripts/netflix_dw.py"

echo ">> Publicando driver JDBC do PostgreSQL"
JDBC_JAR="/tmp/postgresql-42.7.3.jar"
curl --fail --location --silent --show-error \
  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
  --output "${JDBC_JAR}"
awslocal s3 cp "${JDBC_JAR}" "s3://${BUCKET}/scripts/postgresql-42.7.3.jar"

echo ">> Criando role e database do Glue"
awslocal iam create-role \
  --role-name "${ROLE_NAME}" \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  >/dev/null 2>&1 || true
awslocal glue create-database --database-input "{\"Name\":\"${DATABASE}\"}" \
  >/dev/null 2>&1 || true

echo ">> Lendo endpoint RDS para configurar o job Glue"
RDS_STATUS="$(awslocal rds describe-db-instances \
  --db-instance-identifier "${RDS_INSTANCE_ID}" \
  --query 'DBInstances[0].DBInstanceStatus' --output text)"
if [[ "${RDS_STATUS}" != "available" ]]; then
  echo "ERRO: RDS ${RDS_INSTANCE_ID} ainda não está disponível (${RDS_STATUS})." >&2
  exit 1
fi
RDS_PORT="$(awslocal rds describe-db-instances \
  --db-instance-identifier "${RDS_INSTANCE_ID}" \
  --query 'DBInstances[0].Endpoint.Port' --output text)"

echo ">> Registrando job Glue ${JOB_NAME}"
awslocal glue delete-job --job-name "${JOB_NAME}" >/dev/null 2>&1 || true
awslocal glue create-job \
  --name "${JOB_NAME}" \
  --role "${ROLE_NAME}" \
  --glue-version "4.0" \
  --command "Name=glueetl,ScriptLocation=s3://${BUCKET}/scripts/netflix_job.py,PythonVersion=3" \
  --default-arguments "{\
    \"--extra-jars\": \"s3://${BUCKET}/scripts/postgresql-42.7.3.jar\",\
    \"--additional-python-modules\": \"kagglehub\",\
    \"--input_path\": \"${RAW_PATH}\",\
    \"--output_path\": \"${OUTPUT_PATH}\",\
    \"--postgres_host\": \"localstack-dw\",\
    \"--postgres_port\": \"${RDS_PORT}\",\
    \"--postgres_db\": \"${RDS_DB_NAME}\",\
    \"--postgres_user\": \"${RDS_MASTER_USERNAME}\",\
    \"--postgres_password\": \"${RDS_MASTER_PASSWORD}\",\
    \"--postgres_table\": \"public.netflix_titles\",\
    \"--s3_endpoint\": \"http://localstack-dw:4566\"\
  }" >/dev/null

echo ">> Registrando Glue job dimensional ${DW_JOB_NAME}"
awslocal glue delete-job --job-name "${DW_JOB_NAME}" >/dev/null 2>&1 || true
awslocal glue create-job \
  --name "${DW_JOB_NAME}" \
  --role "${ROLE_NAME}" \
  --glue-version "4.0" \
  --command "Name=glueetl,ScriptLocation=s3://${BUCKET}/scripts/netflix_dw.py,PythonVersion=3" \
  --default-arguments "{\
    \"--extra-jars\": \"s3://${BUCKET}/scripts/postgresql-42.7.3.jar\",\
    \"--postgres_host\": \"localstack-dw\",\
    \"--postgres_port\": \"${RDS_PORT}\",\
    \"--postgres_db\": \"${RDS_DB_NAME}\",\
    \"--postgres_user\": \"${RDS_MASTER_USERNAME}\",\
    \"--postgres_password\": \"${RDS_MASTER_PASSWORD}\",\
    \"--source_table\": \"public.netflix_titles\",\
    \"--target_schema\": \"dw_netflix\"\
  }" >/dev/null

echo ">> Registrando crawler para ${OUTPUT_PATH}"
awslocal glue delete-crawler --name "${CRAWLER_NAME}" >/dev/null 2>&1 || true
awslocal glue create-crawler \
  --name "${CRAWLER_NAME}" \
  --role "${ROLE_NAME}" \
  --database-name "${DATABASE}" \
  --targets "{\"S3Targets\":[{\"Path\":\"${OUTPUT_PATH}\"}]}" \
  >/dev/null

echo ">> Job pronto: ${JOB_NAME}"
echo ">> Job dimensional pronto: ${DW_JOB_NAME}"
echo ">> O job baixa o CSV Netflix do Kaggle e publica em ${RAW_PATH}."
echo ">> RDS JDBC: jdbc:postgresql://localstack-dw:${RDS_PORT}/${RDS_DB_NAME}"
