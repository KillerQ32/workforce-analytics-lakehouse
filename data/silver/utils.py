"""
Shared Silver-layer utilities and PySpark UDFs.

This module contains reusable functions used by both the BLS and O*NET
Silver transformations.

Responsibilities include:

- Creating or retrieving a Spark session
- Reading Bronze text and Excel files
- Standardizing SOC and O*NET-SOC codes
- Normalizing column names
- Cleaning whitespace and null placeholders
- Casting data types safely
- Validating required fields and formats
- Detecting and removing duplicate records
- Creating data-quality issue records
- Creating the Unity Catalog Silver schema
- Writing managed Delta Lake tables
"""

import re
from datetime import datetime, timezone
from functools import reduce
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


CATALOG_NAME = "workforce_analytics"
SILVER_SCHEMA = "silver"

NULL_TOKENS = (
    "",
    "—",
    "–",
    "-",
    "N/A",
    "n/a",
    "NA",
    "null",
    "NULL",
)


ISSUE_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("source_system", T.StringType(), False),
        T.StructField("dataset_name", T.StringType(), False),
        T.StructField("source_file_path", T.StringType(), False),
        T.StructField("record_key", T.StringType(), True),
        T.StructField("severity", T.StringType(), False),
        T.StructField("rule_name", T.StringType(), False),
        T.StructField("issue_message", T.StringType(), False),
        T.StructField("raw_value", T.StringType(), True),
        T.StructField("detected_at", T.TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    """
    Return the Spark session used by the Silver pipeline.

    Databricks normally provides an active Spark session. This function
    reuses that session when available. If the file is executed as a
    standalone Databricks Python task, it creates a Spark session.

    Returns:
        SparkSession:
            The active or newly created Spark session.
    """
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("workforce-analytics-silver")
            .getOrCreate()
        )

    return spark


# ---------------------------------------------------------------------------
# PySpark UDFs
# ---------------------------------------------------------------------------

@F.udf(returnType=T.StringType())
def normalize_soc_code_udf(value):
    """
    Normalize a BLS or O*NET occupation code to base SOC format ##-####.

    Examples:
        151252     -> 15-1252
        15-1252    -> 15-1252
        15-1252.00 -> 15-1252

    Args:
        value:
            Raw BLS occupation code, SOC code, or O*NET-SOC code.

    Returns:
        str or None:
            Normalized SOC code, or None when the value is invalid.
    """
    if value is None:
        return None

    text = str(value).strip()

    formatted_match = re.fullmatch(
        r"(\d{2})-(\d{4})(?:\.\d{2})?",
        text,
    )

    if formatted_match:
        return (
            f"{formatted_match.group(1)}-"
            f"{formatted_match.group(2)}"
        )

    digits = re.sub(r"\D", "", text)

    if len(digits) >= 6:
        return f"{digits[:2]}-{digits[2:6]}"

    return None


@F.udf(returnType=T.StringType())
def normalize_onet_soc_code_udf(value):
    """
    Validate and normalize a detailed O*NET-SOC code as ##-####.##.

    Args:
        value:
            Raw detailed O*NET-SOC code.

    Returns:
        str or None:
            Normalized O*NET-SOC code, or None when invalid.
    """
    if value is None:
        return None

    text = str(value).strip()

    match = re.fullmatch(
        r"(\d{2})-(\d{4})\.(\d{2})",
        text,
    )

    if not match:
        return None

    return (
        f"{match.group(1)}-"
        f"{match.group(2)}."
        f"{match.group(3)}"
    )


@F.udf(returnType=T.BooleanType())
def yes_no_to_boolean_udf(value):
    """
    Convert common yes/no source values into Boolean values.

    Args:
        value:
            Raw source value.

    Returns:
        bool or None:
            Converted Boolean value, or None when unrecognized.
    """
    if value is None:
        return None

    normalized = str(value).strip().lower()

    if normalized in {"y", "yes", "true", "t", "1"}:
        return True

    if normalized in {"n", "no", "false", "f", "0"}:
        return False

    return None


# ---------------------------------------------------------------------------
# Bronze file readers
# ---------------------------------------------------------------------------

def read_delimited_file(
    spark: SparkSession,
    path: str,
    delimiter: str = "\t",
) -> DataFrame:
    """
    Read a header-based delimited Bronze file using PySpark.

    All columns are initially kept as strings so Silver can explicitly cast
    values and record conversion failures.

    Args:
        spark:
            Active Spark session.

        path:
            Unity Catalog Volume path to the Bronze source file.

        delimiter:
            Source field delimiter. Defaults to a tab.

    Returns:
        DataFrame:
            Raw Spark DataFrame.
    """
    return (
        spark.read
        .option("header", True)
        .option("sep", delimiter)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("inferSchema", False)
        .csv(path)
    )


def read_excel_sheet_as_spark(
    spark: SparkSession,
    path: str,
    sheet_name: str,
    header_row: int,
) -> DataFrame:
    """
    Read one Excel worksheet and convert it to a Spark DataFrame.

    Pandas and openpyxl are used only to open the XLSX worksheet. Cleaning,
    validation, deduplication, and Delta writes remain PySpark operations.

    Args:
        spark:
            Active Spark session.

        path:
            Unity Catalog Volume path to the Excel workbook.

        sheet_name:
            Worksheet name.

        header_row:
            Zero-based row containing the worksheet headers.

    Returns:
        DataFrame:
            Spark DataFrame containing non-empty worksheet rows.

    Raises:
        RuntimeError:
            If pandas or openpyxl is unavailable.

        ValueError:
            If the selected worksheet contains no usable rows.
    """
    try:
        pandas_df = pd.read_excel(
            path,
            sheet_name=sheet_name,
            header=header_row,
            dtype=str,
            keep_default_na=False,
            engine="openpyxl",
        )

    except ImportError as error:
        raise RuntimeError(
            "Reading BLS Excel files requires pandas and openpyxl."
        ) from error

    pandas_df = pandas_df.replace(
        r"^\s*$",
        pd.NA,
        regex=True,
    )

    pandas_df = pandas_df.dropna(
        axis=0,
        how="all",
    )

    pandas_df = pandas_df.dropna(
        axis=1,
        how="all",
    )

    pandas_df = pandas_df.astype(object).where(
        pd.notnull(pandas_df),
        None,
    )

    if pandas_df.empty:
        raise ValueError(
            f"No usable rows were found in {path}, "
            f"worksheet {sheet_name!r}."
        )

    return spark.createDataFrame(pandas_df)


# ---------------------------------------------------------------------------
# Column and value cleaning
# ---------------------------------------------------------------------------

def normalize_column_name(name: str) -> str:
    """
    Convert one source column name into lowercase snake_case.

    Args:
        name:
            Original source column name.

    Returns:
        str:
            Normalized column name.
    """
    value = str(name).strip().lower()
    value = value.replace("*", "")
    value = re.sub(
        r"[\[\]\(\),/%–—\-]+",
        " ",
        value,
    )
    value = re.sub(
        r"[^a-z0-9]+",
        "_",
        value,
    )
    value = re.sub(
        r"_+",
        "_",
        value,
    )

    return value.strip("_")


def normalize_columns(df: DataFrame) -> DataFrame:
    """
    Normalize every DataFrame column name and detect collisions.

    Args:
        df:
            Input Spark DataFrame.

    Returns:
        DataFrame:
            DataFrame with normalized column names.

    Raises:
        ValueError:
            If multiple source columns normalize to the same name.
    """
    normalized_names = [
        normalize_column_name(column)
        for column in df.columns
    ]

    duplicate_names = {
        normalized_name
        for normalized_name in normalized_names
        if normalized_names.count(normalized_name) > 1
    }

    if duplicate_names:
        raise ValueError(
            "Column normalization produced duplicate names: "
            f"{sorted(duplicate_names)}"
        )

    output = df

    for old_name, new_name in zip(
        df.columns,
        normalized_names,
    ):
        output = output.withColumnRenamed(
            old_name,
            new_name,
        )

    return output


def trim_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim leading and trailing whitespace from every string column.

    Args:
        df:
            Input Spark DataFrame.

    Returns:
        DataFrame:
            DataFrame with trimmed strings.
    """
    output = df

    for field in output.schema.fields:
        if isinstance(field.dataType, T.StringType):
            output = output.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    return output


def nullify_placeholders(
    df: DataFrame,
    null_tokens: Sequence[str] = NULL_TOKENS,
) -> DataFrame:
    """
    Replace known missing-value placeholders with Spark null values.

    Args:
        df:
            Input Spark DataFrame.

        null_tokens:
            Source strings that should be interpreted as missing.

    Returns:
        DataFrame:
            DataFrame with standardized null values.
    """
    output = df

    normalized_tokens = [
        token.strip().lower()
        for token in null_tokens
    ]

    for field in output.schema.fields:
        if isinstance(field.dataType, T.StringType):
            output = output.withColumn(
                field.name,
                F.when(
                    F.lower(
                        F.trim(F.col(field.name))
                    ).isin(normalized_tokens),
                    F.lit(None),
                ).otherwise(F.col(field.name)),
            )

    return output


def add_metadata(
    df: DataFrame,
    source_file_path: str,
    run_id: str,
) -> DataFrame:
    """
    Add Bronze lineage and Silver processing metadata.

    Args:
        df:
            Input Spark DataFrame.

        source_file_path:
            Bronze source file path.

        run_id:
            Unique Silver pipeline run identifier.

    Returns:
        DataFrame:
            DataFrame with source_file_path, run_id, and processed_at.
    """
    return (
        df.withColumn(
            "source_file_path",
            F.lit(source_file_path),
        )
        .withColumn(
            "run_id",
            F.lit(run_id),
        )
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )
    )


def add_soc_columns(
    df: DataFrame,
    onet_column: Optional[str] = None,
    bls_column: Optional[str] = None,
) -> DataFrame:
    """
    Add standardized SOC join-key columns.

    Args:
        df:
            Input Spark DataFrame.

        onet_column:
            Source column containing a detailed O*NET-SOC code.

        bls_column:
            Source column containing a BLS occupation code.

    Returns:
        DataFrame:
            DataFrame containing standardized SOC fields.
    """
    output = df

    if onet_column and onet_column in output.columns:
        output = (
            output.withColumn(
                "onet_soc_code",
                normalize_onet_soc_code_udf(
                    F.col(onet_column)
                ),
            )
            .withColumn(
                "soc_code",
                normalize_soc_code_udf(
                    F.col(onet_column)
                ),
            )
        )

    if bls_column and bls_column in output.columns:
        output = output.withColumn(
            "soc_code",
            normalize_soc_code_udf(
                F.col(bls_column)
            ),
        )

    return output


# ---------------------------------------------------------------------------
# Data-quality issue helpers
# ---------------------------------------------------------------------------

def empty_issues_df(
    spark: SparkSession,
) -> DataFrame:
    """
    Create an empty DataFrame using the shared issue schema.

    Args:
        spark:
            Active Spark session.

    Returns:
        DataFrame:
            Empty issue DataFrame.
    """
    return spark.createDataFrame(
        [],
        ISSUE_SCHEMA,
    )


def record_key_expression(
    key_columns: Sequence[str],
) -> Column:
    """
    Build a readable composite record-key Spark expression.

    Args:
        key_columns:
            Existing columns that form the logical record key.

    Returns:
        Column:
            Spark expression containing the composite key.
    """
    if not key_columns:
        return F.lit(None).cast("string")

    key_values = [
        F.coalesce(
            F.col(column).cast("string"),
            F.lit("<null>"),
        )
        for column in key_columns
    ]

    return F.concat_ws(
        "|",
        *key_values,
    )


def required_value_issues(
    df: DataFrame,
    required_columns: Sequence[str],
    source_system: str,
    dataset_name: str,
    source_file_path: str,
    run_id: str,
    key_columns: Sequence[str],
) -> DataFrame:
    """
    Create issues for missing columns and null required values.

    Args:
        df:
            DataFrame being validated.

        required_columns:
            Columns required for a valid Silver row.

        source_system:
            Source label such as BLS or O*NET.

        dataset_name:
            Target Silver dataset name.

        source_file_path:
            Bronze source path.

        run_id:
            Current pipeline run identifier.

        key_columns:
            Columns used to identify records.

    Returns:
        DataFrame:
            Required-field quality issues.
    """
    issue_frames: List[DataFrame] = []

    available_key_columns = [
        column
        for column in key_columns
        if column in df.columns
    ]

    for required_column in required_columns:
        if required_column not in df.columns:
            detected_at = datetime.now(
                timezone.utc
            ).replace(tzinfo=None)

            missing_column_issue = (
                df.sparkSession.createDataFrame(
                    [
                        (
                            run_id,
                            source_system,
                            dataset_name,
                            source_file_path,
                            None,
                            "error",
                            "missing_required_column",
                            (
                                f"Required column "
                                f"{required_column!r} was not found."
                            ),
                            required_column,
                            detected_at,
                        )
                    ],
                    ISSUE_SCHEMA,
                )
            )

            issue_frames.append(
                missing_column_issue
            )

            continue

        missing_value_issues = (
            df.filter(
                F.col(required_column).isNull()
            )
            .select(
                F.lit(run_id).alias("run_id"),
                F.lit(source_system).alias(
                    "source_system"
                ),
                F.lit(dataset_name).alias(
                    "dataset_name"
                ),
                F.lit(source_file_path).alias(
                    "source_file_path"
                ),
                record_key_expression(
                    available_key_columns
                ).alias("record_key"),
                F.lit("error").alias("severity"),
                F.lit(
                    "missing_required_value"
                ).alias("rule_name"),
                F.lit(
                    f"Required field "
                    f"{required_column!r} is null."
                ).alias("issue_message"),
                F.lit(required_column).alias(
                    "raw_value"
                ),
                F.current_timestamp().alias(
                    "detected_at"
                ),
            )
        )

        issue_frames.append(
            missing_value_issues
        )

    return union_issue_frames(
        df.sparkSession,
        issue_frames,
    )


def invalid_regex_issues(
    df: DataFrame,
    column: str,
    pattern: str,
    source_system: str,
    dataset_name: str,
    source_file_path: str,
    run_id: str,
    key_columns: Sequence[str],
    severity: str = "error",
) -> DataFrame:
    """
    Create issues for non-null values that fail a regex validation rule.

    Args:
        df:
            DataFrame being validated.

        column:
            Column to validate.

        pattern:
            Regular expression valid values must match.

        source_system:
            Source label.

        dataset_name:
            Target Silver dataset name.

        source_file_path:
            Bronze source path.

        run_id:
            Current pipeline run identifier.

        key_columns:
            Columns used to identify records.

        severity:
            Issue severity.

    Returns:
        DataFrame:
            Format-validation issues.
    """
    if column not in df.columns:
        return empty_issues_df(
            df.sparkSession
        )

    available_key_columns = [
        key_column
        for key_column in key_columns
        if key_column in df.columns
    ]

    return (
        df.filter(
            F.col(column).isNotNull()
            & ~F.col(column).rlike(pattern)
        )
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit(source_system).alias(
                "source_system"
            ),
            F.lit(dataset_name).alias(
                "dataset_name"
            ),
            F.lit(source_file_path).alias(
                "source_file_path"
            ),
            record_key_expression(
                available_key_columns
            ).alias("record_key"),
            F.lit(severity).alias("severity"),
            F.lit("invalid_format").alias(
                "rule_name"
            ),
            F.lit(
                f"{column!r} does not match "
                f"the expected pattern {pattern!r}."
            ).alias("issue_message"),
            F.col(column).cast("string").alias(
                "raw_value"
            ),
            F.current_timestamp().alias(
                "detected_at"
            ),
        )
    )


def cast_columns_with_issues(
    df: DataFrame,
    cast_map: Dict[str, str],
    source_system: str,
    dataset_name: str,
    source_file_path: str,
    run_id: str,
    key_columns: Sequence[str],
) -> Tuple[DataFrame, DataFrame]:
    """
    Safely cast selected columns and record conversion failures.

    Databricks SQL try_cast is used so malformed values become null instead
    of stopping the pipeline. Dollar signs, commas, and percent signs are
    removed before numeric conversion.

    Dataset-specific qualifiers such as >= should be handled before calling
    this function so their meaning can be preserved.

    Args:
        df:
            DataFrame containing source columns.

        cast_map:
            Mapping of column names to Spark target types.

        source_system:
            Source label.

        dataset_name:
            Target Silver dataset name.

        source_file_path:
            Bronze source path.

        run_id:
            Current pipeline run identifier.

        key_columns:
            Columns used to identify cast failures.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Converted DataFrame and conversion-issue DataFrame.
    """
    output = df
    issue_frames: List[DataFrame] = []

    available_key_columns = [
        column
        for column in key_columns
        if column in output.columns
    ]

    numeric_type_prefixes = (
        "byte",
        "short",
        "int",
        "integer",
        "long",
        "bigint",
        "float",
        "double",
        "decimal",
    )

    for column, target_type in cast_map.items():
        if column not in output.columns:
            continue

        raw_column = f"__raw_{column}"
        cleaned_column = f"__cleaned_{column}"

        output = output.withColumn(
            raw_column,
            F.col(column).cast("string"),
        )

        cleaned_expression = F.trim(
            F.col(raw_column)
        )

        normalized_target_type = (
            target_type.strip().lower()
        )

        if normalized_target_type.startswith(
            numeric_type_prefixes
        ):
            cleaned_expression = F.regexp_replace(
                cleaned_expression,
                r"[$,%]",
                "",
            )

        output = output.withColumn(
            cleaned_column,
            cleaned_expression,
        )

        escaped_cleaned_column = (
            cleaned_column.replace(
                "`",
                "``",
            )
        )

        output = output.withColumn(
            column,
            F.expr(
                f"try_cast("
                f"`{escaped_cleaned_column}` "
                f"AS {target_type})"
            ),
        )

        cast_issues = (
            output.filter(
                F.col(raw_column).isNotNull()
                & F.col(column).isNull()
            )
            .select(
                F.lit(run_id).alias("run_id"),
                F.lit(source_system).alias(
                    "source_system"
                ),
                F.lit(dataset_name).alias(
                    "dataset_name"
                ),
                F.lit(source_file_path).alias(
                    "source_file_path"
                ),
                record_key_expression(
                    available_key_columns
                ).alias("record_key"),
                F.lit("error").alias("severity"),
                F.lit(
                    "invalid_type_cast"
                ).alias("rule_name"),
                F.lit(
                    f"Could not cast {column!r} "
                    f"to {target_type!r}."
                ).alias("issue_message"),
                F.col(raw_column).alias(
                    "raw_value"
                ),
                F.current_timestamp().alias(
                    "detected_at"
                ),
            )
        )

        issue_frames.append(
            cast_issues
        )

        output = output.drop(
            raw_column,
            cleaned_column,
        )

    return (
        output,
        union_issue_frames(
            df.sparkSession,
            issue_frames,
        ),
    )


def deduplicate_with_issues(
    df: DataFrame,
    key_columns: Sequence[str],
    source_system: str,
    dataset_name: str,
    source_file_path: str,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Remove duplicate business keys and record duplicate issues.

    Exact duplicate rows generate a warning. Rows with the same business key
    but conflicting values generate an error. One deterministic row is kept
    using a SHA-256 hash of the source values.

    Args:
        df:
            DataFrame to deduplicate.

        key_columns:
            Columns that identify a logical record.

        source_system:
            Source label.

        dataset_name:
            Target Silver dataset name.

        source_file_path:
            Bronze source path.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Deduplicated DataFrame and duplicate-issue DataFrame.
    """
    available_keys = [
        column
        for column in key_columns
        if column in df.columns
    ]

    if not available_keys:
        return (
            df,
            empty_issues_df(df.sparkSession),
        )

    data_columns = sorted(
        column
        for column in df.columns
        if column not in {
            "processed_at",
            "run_id",
            "source_file_path",
        }
    )

    row_hash = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(column).cast("string"),
                    F.lit("<null>"),
                )
                for column in data_columns
            ],
        ),
        256,
    )

    hashed_df = df.withColumn(
        "__row_hash",
        row_hash,
    )

    duplicate_summary = (
        hashed_df.groupBy(*available_keys)
        .agg(
            F.count(F.lit(1)).alias(
                "__duplicate_count"
            ),
            F.countDistinct("__row_hash").alias(
                "__distinct_version_count"
            ),
        )
        .filter(
            F.col("__duplicate_count") > 1
        )
    )

    duplicate_issues = duplicate_summary.select(
        F.lit(run_id).alias("run_id"),
        F.lit(source_system).alias(
            "source_system"
        ),
        F.lit(dataset_name).alias(
            "dataset_name"
        ),
        F.lit(source_file_path).alias(
            "source_file_path"
        ),
        record_key_expression(
            available_keys
        ).alias("record_key"),
        F.when(
            F.col("__distinct_version_count") > 1,
            F.lit("error"),
        ).otherwise(
            F.lit("warning")
        ).alias("severity"),
        F.when(
            F.col("__distinct_version_count") > 1,
            F.lit("conflicting_duplicate_key"),
        ).otherwise(
            F.lit("exact_duplicate_key")
        ).alias("rule_name"),
        F.concat(
            F.lit("Found "),
            F.col("__duplicate_count").cast(
                "string"
            ),
            F.lit(
                " rows for the same business key."
            ),
        ).alias("issue_message"),
        F.col("__duplicate_count").cast(
            "string"
        ).alias("raw_value"),
        F.current_timestamp().alias(
            "detected_at"
        ),
    )

    deduplication_window = (
        Window.partitionBy(*available_keys)
        .orderBy(F.col("__row_hash"))
    )

    deduplicated_df = (
        hashed_df.withColumn(
            "__row_number",
            F.row_number().over(
                deduplication_window
            ),
        )
        .filter(
            F.col("__row_number") == 1
        )
        .drop(
            "__row_number",
            "__row_hash",
        )
    )

    return (
        deduplicated_df,
        duplicate_issues,
    )


def filter_valid_required_rows(
    df: DataFrame,
    required_columns: Sequence[str],
) -> DataFrame:
    """
    Keep only rows whose required values are non-null.

    Args:
        df:
            DataFrame being filtered.

        required_columns:
            Required Silver fields.

    Returns:
        DataFrame:
            Rows satisfying all required-field rules.
    """
    existing_columns = [
        column
        for column in required_columns
        if column in df.columns
    ]

    if not existing_columns:
        return df

    required_condition = reduce(
        lambda left, right: left & right,
        [
            F.col(column).isNotNull()
            for column in existing_columns
        ],
    )

    return df.filter(
        required_condition
    )


def union_issue_frames(
    spark: SparkSession,
    frames: Iterable[DataFrame],
) -> DataFrame:
    """
    Combine multiple issue DataFrames using the shared issue schema.

    Args:
        spark:
            Active Spark session.

        frames:
            Iterable of issue DataFrames.

    Returns:
        DataFrame:
            Combined issue DataFrame.
    """
    issue_column_names = [
        field.name
        for field in ISSUE_SCHEMA
    ]

    valid_frames = [
        frame.select(*issue_column_names)
        for frame in frames
        if frame is not None
    ]

    if not valid_frames:
        return empty_issues_df(
            spark
        )

    return reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=False,
        ),
        valid_frames,
    )


# ---------------------------------------------------------------------------
# Unity Catalog and Delta Lake writers
# ---------------------------------------------------------------------------

def ensure_silver_schema(
    spark: SparkSession,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SILVER_SCHEMA,
) -> None:
    """
    Create the Unity Catalog catalog and Silver schema when missing.

    Args:
        spark:
            Active Spark session.

        catalog_name:
            Unity Catalog catalog name.

        schema_name:
            Silver schema name.

    Returns:
        None.
    """
    spark.sql(
        f"CREATE CATALOG IF NOT EXISTS {catalog_name}"
    )

    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS
        {catalog_name}.{schema_name}
        """
    )


def write_delta_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_columns: Optional[
        Sequence[str]
    ] = None,
) -> None:
    """
    Write a Spark DataFrame as a managed Unity Catalog Delta table.

    Args:
        df:
            Spark DataFrame to write.

        table_name:
            Fully qualified catalog.schema.table name.

        mode:
            Spark write mode, normally overwrite or append.

        partition_columns:
            Optional Delta partition columns.

    Returns:
        None.
    """
    writer = (
        df.write
        .format("delta")
        .mode(mode)
    )

    if mode == "overwrite":
        writer = writer.option(
            "overwriteSchema",
            "true",
        )

    if partition_columns:
        writer = writer.partitionBy(
            *partition_columns
        )

    writer.saveAsTable(
        table_name
    )