"""Carrega o CSV de títulos Netflix do S3 para S3 (Parquet) e RDS PostgreSQL."""
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import boto3
import kagglehub
from pyspark.sql import SparkSession

try:
    from awsglue.context import GlueContext
    RUNNING_ON_GLUE = True
except ImportError:
    RUNNING_ON_GLUE = False


def get_config():
    defaults = {
        "input_path": os.getenv(
            "INPUT_PATH", "s3://dw-datalake/raw/netflix-movies-and-series/"
        ),
        "output_path": os.getenv(
            "OUTPUT_PATH", "s3://dw-datalake/refined/netflix_titles/"
        ),
        # Nome DNS do container LocalStack na rede compartilhada com o job Glue.
        "postgres_host": os.getenv("POSTGRES_HOST", "localstack-dw"),
        "postgres_port": os.getenv("POSTGRES_PORT", "5432"),
        "postgres_db": os.getenv("POSTGRES_DB", "db_dadosbruto"),
        "postgres_user": os.getenv("POSTGRES_USER", "dwadmin"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD", "dwlocal123"),
        "postgres_table": os.getenv("POSTGRES_TABLE", "public.netflix_titles"),
        "s3_endpoint": os.getenv("S3_ENDPOINT_URL", "http://localstack-dw:4566"),
    }
    for argument in sys.argv[1:]:
        if not argument.startswith("--") or "=" not in argument:
            continue
        name, value = argument[2:].split("=", 1)
        name = name.replace("-", "_")
        if name in defaults:
            defaults[name] = value
    return defaults


def create_spark():
    builder = SparkSession.builder.appName("netflix-to-localstack-rds")
    packages = ["org.apache.hadoop:hadoop-aws:3.3.5"]
    if not RUNNING_ON_GLUE:
        # No Glue, o JAR é fornecido pelo argumento --extra-jars do job.
        packages.append("org.postgresql:postgresql:42.7.3")
    builder = builder.config("spark.jars.packages", ",".join(packages))

    return (
        builder
        .config(
            "spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.endpoint", get_config()["s3_endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def upload_csvs_to_s3(dataset_path, s3_path, endpoint_url):
    source = Path(dataset_path)
    csv_files = (
        [source]
        if source.is_file() and source.suffix.lower() == ".csv"
        else sorted(
            file_path
            for file_path in source.rglob("*")
            if file_path.is_file() and file_path.suffix.lower() == ".csv"
        )
    )
    if not csv_files:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {source}")

    target = urlparse(s3_path)
    if target.scheme != "s3" or not target.netloc:
        raise ValueError(f"Caminho S3 inválido: {s3_path}")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )
    prefix = target.path.strip("/")
    for csv_file in csv_files:
        relative_path = (
            csv_file.name
            if source.is_file()
            else csv_file.relative_to(source).as_posix()
        )
        key = "/".join(part for part in (prefix, relative_path) if part)
        s3.upload_file(str(csv_file), target.netloc, key)
        print(f"CSV enviado para s3://{target.netloc}/{key}")


def main():
    config = get_config()
    dataset_path = kagglehub.dataset_download(
        "debayank2024/netflix-movies-and-series"
    )
    print(f"Dataset baixado em: {dataset_path}")
    upload_csvs_to_s3(dataset_path, config["input_path"], config["s3_endpoint"])

    spark = create_spark()
    try:
        if RUNNING_ON_GLUE:
            GlueContext(spark.sparkContext)

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("recursiveFileLookup", True)
            .csv(config["input_path"])
        )
        df.printSchema()
        df.show(50, truncate=False)

        # Mantém uma cópia analítica no S3 e grava a tabela no PostgreSQL do RDS.
        df.write.mode("overwrite").parquet(config["output_path"])

        jdbc_url = (
            f"jdbc:postgresql://{config['postgres_host']}:{config['postgres_port']}"
            f"/{config['postgres_db']}"
        )
        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", config["postgres_table"])
            .option("user", config["postgres_user"])
            .option("password", config["postgres_password"])
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
        print(f"Dados gravados no S3: {config['output_path']}")
        print(f"Dados gravados no RDS: {jdbc_url}/{config['postgres_table']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
