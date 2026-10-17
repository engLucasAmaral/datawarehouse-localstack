#!/usr/bin/env bash
# Cria a instância PostgreSQL do RDS emulado pelo LocalStack.
# Executado pelo LocalStack no ready.d, depois que os serviços estiverem ativos.
set -euo pipefail

DB_INSTANCE_ID="${RDS_INSTANCE_ID:-netflix-postgres}"
DB_NAME="${RDS_DB_NAME:-db_dadosbruto}"
DB_USER="${RDS_MASTER_USERNAME:-dwadmin}"
DB_PASSWORD="${RDS_MASTER_PASSWORD:-dwlocal123}"
DB_CLASS="${RDS_INSTANCE_CLASS:-db.t3.small}"

echo ">> Verificando instância RDS ${DB_INSTANCE_ID}"
STATUS="$(awslocal rds describe-db-instances \
  --db-instance-identifier "${DB_INSTANCE_ID}" \
  --query 'DBInstances[0].DBInstanceStatus' \
  --output text 2>/dev/null || true)"

if [[ -z "${STATUS}" || "${STATUS}" == "None" ]]; then
  echo ">> Criando RDS PostgreSQL ${DB_NAME}"
  awslocal rds create-db-instance \
    --db-instance-identifier "${DB_INSTANCE_ID}" \
    --engine postgres \
    --db-instance-class "${DB_CLASS}" \
    --allocated-storage 20 \
    --db-name "${DB_NAME}" \
    --master-username "${DB_USER}" \
    --master-user-password "${DB_PASSWORD}" >/dev/null
fi

echo ">> Aguardando RDS ficar disponível"
for attempt in $(seq 1 90); do
  STATUS="$(awslocal rds describe-db-instances \
    --db-instance-identifier "${DB_INSTANCE_ID}" \
    --query 'DBInstances[0].DBInstanceStatus' \
    --output text 2>/dev/null || true)"
  if [[ "${STATUS}" == "available" ]]; then
    break
  fi
  if (( attempt == 90 )); then
    echo "ERRO: RDS não ficou disponível; último estado: ${STATUS}" >&2
    exit 1
  fi
  sleep 5
done

DB_ENDPOINT="$(awslocal rds describe-db-instances \
  --db-instance-identifier "${DB_INSTANCE_ID}" \
  --query 'DBInstances[0].Endpoint.Address' \
  --output text)"
DB_PORT="$(awslocal rds describe-db-instances \
  --db-instance-identifier "${DB_INSTANCE_ID}" \
  --query 'DBInstances[0].Endpoint.Port' \
  --output text)"

echo ">> RDS PostgreSQL pronto para o job"
echo "   Host: ${DB_ENDPOINT}"
echo "   Porta: ${DB_PORT}"
echo "   Banco: ${DB_NAME}"
echo "   Usuário: ${DB_USER}"
echo "   JDBC: jdbc:postgresql://${DB_ENDPOINT}:${DB_PORT}/${DB_NAME}"
echo "   Senha: definida por RDS_MASTER_PASSWORD (padrão local: dwlocal123)"
