"""Builds the Netflix dimensional warehouse from public.netflix_titles."""
import os
import re
import sys

from pyspark.sql import SparkSession, Window, functions as F

try:
    from awsglue.context import GlueContext
    RUNNING_ON_GLUE = True
except ImportError:
    RUNNING_ON_GLUE = False


def get_config():
    config = {
        "postgres_host": os.getenv("POSTGRES_HOST", "localstack-dw"),
        "postgres_port": os.getenv("POSTGRES_PORT", "4510"),
        "postgres_db": os.getenv("POSTGRES_DB", "db_dadosbruto"),
        "postgres_user": os.getenv("POSTGRES_USER", "dwadmin"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD", "dwlocal123"),
        "source_table": os.getenv("SOURCE_TABLE", "public.netflix_titles"),
        "target_schema": os.getenv("TARGET_SCHEMA", "dw_netflix"),
    }
    for argument in sys.argv[1:]:
        if not argument.startswith("--") or "=" not in argument:
            continue
        name, value = argument[2:].split("=", 1)
        name = name.replace("-", "_")
        if name in config:
            config[name] = value
    return config


def validate_identifier(identifier):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Identificador SQL inválido: {identifier}")
    return identifier


def create_schema(spark, jdbc_url, config):
    schema = validate_identifier(config["target_schema"])
    jvm = spark._jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    connection = jvm.java.sql.DriverManager.getConnection(
        jdbc_url, config["postgres_user"], config["postgres_password"]
    )
    try:
        statement = connection.createStatement()
        try:
            statement.executeUpdate(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        finally:
            statement.close()
    finally:
        connection.close()


def read_source(spark, jdbc_url, config):
    source_table = config["source_table"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", source_table):
        raise ValueError(f"Tabela de origem inválida: {source_table}")
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", source_table)
        .option("user", config["postgres_user"])
        .option("password", config["postgres_password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def build_dimension(source_df, source_column, value_column, key_column, cast_type=None):
    value = F.trim(F.col(source_column))
    if cast_type:
        value = value.cast(cast_type)
    values = (
        source_df.select(value.alias(value_column))
        .filter(F.col(value_column).isNotNull())
        .filter(F.length(F.trim(F.col(value_column).cast("string"))) > 0)
        .distinct()
    )
    return (
        values.withColumn(
            key_column,
            F.row_number().over(Window.orderBy(F.col(value_column))).cast("long"),
        )
        .select(key_column, value_column)
    )


def split_values(source_df, source_column, value_column):
    return (
        source_df.select(
            "show_id",
            F.explode(F.split(F.coalesce(F.col(source_column), F.lit("")), ",")).alias(
                value_column
            ),
        )
        .withColumn(value_column, F.trim(F.col(value_column)))
        .filter(F.col(value_column) != "")
    )


def build_date_dimension(source_df):
    parsed_date = F.to_date(F.trim(F.col("date_added")), "MMMM d, yyyy")
    dates = (
        source_df.select(parsed_date.alias("data_adicao"))
        .filter(F.col("data_adicao").isNotNull())
        .distinct()
    )
    return dates.select(
        F.date_format("data_adicao", "yyyyMMdd").cast("int").alias("data_adicao_key"),
        "data_adicao",
        F.year("data_adicao").alias("ano"),
        F.month("data_adicao").alias("mes"),
        F.dayofmonth("data_adicao").alias("dia"),
    )


def build_duration_dimension(source_df):
    durations = (
        source_df.select(F.trim(F.col("duration")).alias("duracao"))
        .filter(F.col("duracao").isNotNull())
        .filter(F.col("duracao") != "")
        .distinct()
        .withColumn(
            "duracao_valor",
            F.regexp_extract("duracao", r"^(\d+)", 1).cast("int"),
        )
        .withColumn(
            "unidade_duracao",
            F.when(F.lower("duracao").contains("min"), F.lit("minutos"))
            .when(F.lower("duracao").contains("season"), F.lit("temporadas")),
        )
    )
    return durations.withColumn(
        "duracao_key",
        F.row_number().over(Window.orderBy("duracao")).cast("long"),
    ).select("duracao_key", "duracao", "duracao_valor", "unidade_duracao")


def build_bridge(values_df, dimension_df, value_column, key_column):
    return (
        values_df.join(dimension_df, on=value_column, how="inner")
        .select("show_id", key_column)
        .dropDuplicates(["show_id", key_column])
    )


def build_model(source_df):
    dimensions = {
        "dim_tipo": build_dimension(source_df, "type", "tipo", "tipo_key"),
        "dim_classificacao": build_dimension(
            source_df, "rating", "classificacao", "classificacao_key"
        ),
        "dim_ano_lancamento": build_dimension(
            source_df, "release_year", "ano_lancamento", "ano_lancamento_key", "int"
        ),
        "dim_data_adicao": build_date_dimension(source_df),
        "dim_duracao": build_duration_dimension(source_df),
        "dim_pais": build_dimension(
            split_values(source_df, "country", "pais"), "pais", "pais", "pais_key"
        ),
        "dim_diretor": build_dimension(
            split_values(source_df, "director", "diretor"),
            "diretor", "diretor", "diretor_key",
        ),
        "dim_ator": build_dimension(
            split_values(source_df, "cast", "ator"), "ator", "ator", "ator_key"
        ),
        "dim_categoria": build_dimension(
            split_values(source_df, "listed_in", "categoria"),
            "categoria", "categoria", "categoria_key",
        ),
    }

    date_value = F.to_date(F.trim(F.col("date_added")), "MMMM d, yyyy")
    fact_source = (
        source_df.select(
            "show_id",
            "title",
            "description",
            F.trim(F.col("type")).alias("tipo"),
            F.trim(F.col("rating")).alias("classificacao"),
            F.col("release_year").cast("int").alias("ano_lancamento"),
            F.date_format(date_value, "yyyyMMdd").cast("int").alias("data_adicao_key"),
            F.trim(F.col("duration")).alias("duracao"),
        )
        .filter(F.col("show_id").isNotNull())
    )
    fact = (
        fact_source
        .join(dimensions["dim_tipo"], on="tipo", how="left")
        .join(dimensions["dim_classificacao"], on="classificacao", how="left")
        .join(dimensions["dim_ano_lancamento"], on="ano_lancamento", how="left")
        .join(dimensions["dim_duracao"], on="duracao", how="left")
        .select(
            "show_id",
            "title",
            "description",
            "tipo_key",
            "classificacao_key",
            "ano_lancamento_key",
            "data_adicao_key",
            "duracao_key",
            F.lit(1).alias("quantidade_titulos"),
        )
    )

    bridges = {
        "bridge_titulo_pais": build_bridge(
            split_values(source_df, "country", "pais"),
            dimensions["dim_pais"], "pais", "pais_key",
        ),
        "bridge_titulo_diretor": build_bridge(
            split_values(source_df, "director", "diretor"),
            dimensions["dim_diretor"], "diretor", "diretor_key",
        ),
        "bridge_titulo_ator": build_bridge(
            split_values(source_df, "cast", "ator"),
            dimensions["dim_ator"], "ator", "ator_key",
        ),
        "bridge_titulo_categoria": build_bridge(
            split_values(source_df, "listed_in", "categoria"),
            dimensions["dim_categoria"], "categoria", "categoria_key",
        ),
    }
    return dimensions, fact, bridges


def write_table(dataframe, table_name, jdbc_url, config):
    table_name = validate_identifier(table_name)
    target = f"{config['target_schema']}.{table_name}"
    (
        dataframe.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", target)
        .option("user", config["postgres_user"])
        .option("password", config["postgres_password"])
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    print(f"Tabela carregada: {target}")


def main():
    config = get_config()
    config["target_schema"] = validate_identifier(config["target_schema"])
    spark = SparkSession.builder.appName("netflix-dimensional-warehouse").getOrCreate()
    jdbc_url = (
        f"jdbc:postgresql://{config['postgres_host']}:{config['postgres_port']}"
        f"/{config['postgres_db']}"
    )
    try:
        if RUNNING_ON_GLUE:
            GlueContext(spark.sparkContext)
        create_schema(spark, jdbc_url, config)
        source_df = read_source(spark, jdbc_url, config)
        dimensions, fact, bridges = build_model(source_df)

        for table_name, dataframe in dimensions.items():
            write_table(dataframe, table_name, jdbc_url, config)
        write_table(fact, "fato_titulo", jdbc_url, config)
        for table_name, dataframe in bridges.items():
            write_table(dataframe, table_name, jdbc_url, config)

        print(
            f"Data warehouse carregado em {config['target_schema']} "
            f"a partir de {config['source_table']}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()