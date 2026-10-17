Coloque aqui o driver JDBC do PostgreSQL usado pelo job Glue (Spark) para
gravar as tabelas finais em glue_dw.* no Postgres.

Baixe com:
  curl -L -o postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

Este arquivo é lido pelo docker-compose-localstack.yml e enviado para
s3://dw-datalake/scripts/postgresql.jar durante o init do LocalStack.
