"""
O*NET Silver-layer table transformations.

This module reads the raw O*NET tab-delimited files stored in the Bronze
Unity Catalog Volume and creates cleaned, validated, deduplicated Spark
DataFrames.

The module handles:

- Occupation data
- Education categories
- Education ratings
- Training and experience ratings
- Essential skills
- Transferable skills
- Software skills
- Knowledge
- Abilities
- Work activities
- Task statements
- Job zones
- Related occupations

Detailed O*NET-SOC codes are preserved. A base SOC code is also created
for future joins with BLS tables in the Gold layer.
"""

from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from data.silver.utils import (
    add_metadata,
    cast_columns_with_issues,
    deduplicate_with_issues,
    filter_valid_required_rows,
    invalid_regex_issues,
    normalize_columns,
    normalize_onet_soc_code_udf,
    normalize_soc_code_udf,
    nullify_placeholders,
    read_delimited_file,
    required_value_issues,
    trim_string_columns,
    union_issue_frames,
    yes_no_to_boolean_udf,
)

ONET_BRONZE_BASE = (
    "/Volumes/workforce_analytics/bronze/raw_files/"
    "onet/text_files"
)


def _base_onet_frame(
    spark: SparkSession,
    file_name: str,
    run_id: str,
) -> Tuple[DataFrame, str]:
    """
    Read and apply base cleaning to one O*NET text file.

    The function reads the tab-delimited source file, normalizes headers,
    trims whitespace, converts known placeholders to null, and adds Silver
    lineage metadata.

    Args:
        spark:
            Active Spark session.

        file_name:
            O*NET text filename inside the Bronze text_files directory.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, str]:
            Base-cleaned O*NET DataFrame and its Bronze source path.
    """
    path = f"{ONET_BRONZE_BASE}/{file_name}"

    df = read_delimited_file(
        spark=spark,
        path=path,
        delimiter="\t",
    )

    df = normalize_columns(df)
    df = trim_string_columns(df)
    df = nullify_placeholders(df)
    df = add_metadata(df, path, run_id)

    return df, path


def _add_onet_soc_fields(
    df: DataFrame,
) -> DataFrame:
    """
    Preserve the raw O*NET-SOC code and create standardized SOC fields.

    The original source code is renamed to onet_soc_code_raw. The function
    then creates:

    - onet_soc_code in ##-####.## format
    - soc_code in ##-#### format

    Args:
        df:
            O*NET DataFrame containing an onet_soc_code source column.

    Returns:
        DataFrame:
            DataFrame containing raw, detailed, and base SOC fields.

    Raises:
        ValueError:
            Raised when the source does not contain onet_soc_code.
    """
    if "onet_soc_code" not in df.columns:
        raise ValueError(
            "The O*NET source file does not contain "
            "'O*NET-SOC Code'. "
            f"Available columns: {df.columns}"
        )

    return (
        df.withColumnRenamed(
            "onet_soc_code",
            "onet_soc_code_raw",
        )
        .withColumn(
            "onet_soc_code",
            normalize_onet_soc_code_udf(
                F.col("onet_soc_code_raw")
            ),
        )
        .withColumn(
            "soc_code",
            normalize_soc_code_udf(
                F.col("onet_soc_code_raw")
            ),
        )
    )


def _add_source_date(
    df: DataFrame,
) -> DataFrame:
    """
    Convert an O*NET MM/YYYY source date into a Spark date value.

    The original source value is preserved as source_date_raw. The clean
    data_month field uses the first day of the source month.

    Args:
        df:
            O*NET DataFrame that may contain a date column.

    Returns:
        DataFrame:
            DataFrame containing source_date_raw and data_month when the
            source date exists.
    """
    if "date" not in df.columns:
        return df

    return (
        df.withColumnRenamed(
            "date",
            "source_date_raw",
        )
        .withColumn(
            "data_month",
            F.to_date(
                F.concat(
                    F.lit("01/"),
                    F.col("source_date_raw"),
                ),
                "dd/MM/yyyy",
            ),
        )
    )


def _select_existing_columns(
    df: DataFrame,
    columns: Sequence[str],
) -> DataFrame:
    """
    Select only requested columns that exist in a DataFrame.

    This supports small differences between O*NET files without attempting
    to invent missing source values.

    Args:
        df:
            Source DataFrame.

        columns:
            Ordered list of desired columns.

    Returns:
        DataFrame:
            DataFrame containing the requested existing columns.
    """
    existing_columns = [
        column
        for column in columns
        if column in df.columns
    ]

    return df.select(
        *[
            F.col(column)
            for column in existing_columns
        ]
    )


def _finish_onet_table(
    df: DataFrame,
    dataset_name: str,
    source_file_path: str,
    run_id: str,
    required_columns: Sequence[str],
    key_columns: Sequence[str],
    extra_issue_frames: Optional[
        List[DataFrame]
    ] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Apply shared validation and deduplication to an O*NET Silver table.

    The function validates required values and raw O*NET-SOC formats,
    excludes invalid required records from the clean result, detects
    duplicate business keys, and combines all generated issues.

    Args:
        df:
            O*NET DataFrame to validate.

        dataset_name:
            Target Silver dataset name.

        source_file_path:
            Bronze source file path.

        run_id:
            Unique identifier for the current Silver pipeline run.

        required_columns:
            Fields that must be non-null for a valid Silver record.

        key_columns:
            Columns defining the logical record key.

        extra_issue_frames:
            Optional dataset-specific issue DataFrames.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean deduplicated records and combined quality issues.
    """
    issue_frames: List[DataFrame] = list(
        extra_issue_frames or []
    )

    required_issues = required_value_issues(
        df=df,
        required_columns=required_columns,
        source_system="O*NET",
        dataset_name=dataset_name,
        source_file_path=source_file_path,
        run_id=run_id,
        key_columns=key_columns,
    )

    issue_frames.append(required_issues)

    if "onet_soc_code_raw" in df.columns:
        onet_format_issues = invalid_regex_issues(
            df=df,
            column="onet_soc_code_raw",
            pattern=r"^\d{2}-\d{4}\.\d{2}$",
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=source_file_path,
            run_id=run_id,
            key_columns=key_columns,
        )

        issue_frames.append(
            onet_format_issues
        )

    valid_df = filter_valid_required_rows(
        df=df,
        required_columns=required_columns,
    )

    valid_df, duplicate_issues = (
        deduplicate_with_issues(
            df=valid_df,
            key_columns=key_columns,
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=source_file_path,
            run_id=run_id,
        )
    )

    issue_frames.append(duplicate_issues)

    all_issues = union_issue_frames(
        spark=df.sparkSession,
        frames=issue_frames,
    )

    return valid_df, all_issues


def build_onet_occupations(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the core O*NET occupation table.

    The source contains the detailed O*NET-SOC code, occupation title, and
    occupation description. A base SOC code is created for future BLS joins.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean O*NET occupation records and associated quality issues.
    """
    dataset_name = "onet_occupations"

    df, path = _base_onet_frame(
        spark=spark,
        file_name="Occupation Data.txt",
        run_id=run_id,
    )

    df = _add_onet_soc_fields(df)

    selected = df.select(
        F.col("onet_soc_code_raw"),
        F.col("onet_soc_code"),
        F.col("soc_code"),
        F.col("title").alias(
            "occupation_title"
        ),
        F.col("description"),
        F.col("source_file_path"),
        F.col("run_id"),
        F.col("processed_at"),
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "occupation_title",
        ],
        key_columns=["onet_soc_code"],
    )


def build_onet_education_categories(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET education-category reference table.

    This lookup maps the Required Level of Education scale and its numeric
    categories to readable education descriptions.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean education categories and associated quality issues.
    """
    dataset_name = "onet_education_categories"

    df, path = _base_onet_frame(
        spark=spark,
        file_name="Education Categories.txt",
        run_id=run_id,
    )

    selected = df.select(
        F.col("element_id"),
        F.col("element_name"),
        F.col("scale_id"),
        F.col("category"),
        F.col("category_description"),
        F.col("source_file_path"),
        F.col("run_id"),
        F.col("processed_at"),
    )

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map={
                "category": "int",
            },
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "element_id",
                "scale_id",
                "category",
            ],
        )
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "element_id",
            "scale_id",
            "category",
            "category_description",
        ],
        key_columns=[
            "element_id",
            "scale_id",
            "category",
        ],
        extra_issue_frames=[cast_issues],
    )


def _build_rating_table(
    spark: SparkSession,
    run_id: str,
    file_name: str,
    dataset_name: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build a standard O*NET occupation-rating table.

    Essential Skills, Transferable Skills, Knowledge, Abilities, and Work
    Activities share a similar source structure. This function cleans that
    common structure, converts numeric fields, converts suppression flags,
    parses the source month, validates SOC codes, and removes duplicates.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

        file_name:
            O*NET source filename.

        dataset_name:
            Target Silver table name.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean rating records and associated quality issues.
    """
    df, path = _base_onet_frame(
        spark=spark,
        file_name=file_name,
        run_id=run_id,
    )

    df = _add_onet_soc_fields(df)
    df = _add_source_date(df)

    for boolean_column in (
        "recommend_suppress",
        "not_relevant",
    ):
        if boolean_column in df.columns:
            df = df.withColumn(
                boolean_column,
                yes_no_to_boolean_udf(
                    F.col(boolean_column)
                ),
            )

    selected = _select_existing_columns(
        df=df,
        columns=[
            "onet_soc_code_raw",
            "onet_soc_code",
            "soc_code",
            "element_id",
            "element_name",
            "scale_id",
            "data_value",
            "n",
            "standard_error",
            "lower_ci_bound",
            "upper_ci_bound",
            "recommend_suppress",
            "not_relevant",
            "source_date_raw",
            "data_month",
            "domain_source",
            "source_file_path",
            "run_id",
            "processed_at",
        ],
    )

    numeric_columns = {
        "data_value": "double",
        "n": "int",
        "standard_error": "double",
        "lower_ci_bound": "double",
        "upper_ci_bound": "double",
    }

    existing_numeric_columns = {
        column: data_type
        for column, data_type in numeric_columns.items()
        if column in selected.columns
    }

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map=existing_numeric_columns,
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "onet_soc_code",
                "element_id",
                "scale_id",
            ],
        )
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "element_id",
            "scale_id",
            "data_value",
        ],
        key_columns=[
            "onet_soc_code",
            "element_id",
            "scale_id",
        ],
        extra_issue_frames=[cast_issues],
    )


def _build_category_rating_table(
    spark: SparkSession,
    run_id: str,
    file_name: str,
    dataset_name: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build an O*NET category-based occupation-rating table.

    Education and Training and Experience ratings include a category in
    addition to the occupation, element, and scale identifiers. This
    function cleans that structure, parses numeric values and source dates,
    validates occupation codes, and removes duplicate category records.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

        file_name:
            O*NET source filename.

        dataset_name:
            Target Silver table name.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean category-rating records and quality issues.
    """
    df, path = _base_onet_frame(
        spark=spark,
        file_name=file_name,
        run_id=run_id,
    )

    df = _add_onet_soc_fields(df)
    df = _add_source_date(df)

    if "recommend_suppress" in df.columns:
        df = df.withColumn(
            "recommend_suppress",
            yes_no_to_boolean_udf(
                F.col("recommend_suppress")
            ),
        )

    selected = _select_existing_columns(
        df=df,
        columns=[
            "onet_soc_code_raw",
            "onet_soc_code",
            "soc_code",
            "element_id",
            "element_name",
            "scale_id",
            "category",
            "data_value",
            "n",
            "standard_error",
            "lower_ci_bound",
            "upper_ci_bound",
            "recommend_suppress",
            "source_date_raw",
            "data_month",
            "domain_source",
            "source_file_path",
            "run_id",
            "processed_at",
        ],
    )

    numeric_columns = {
        "category": "int",
        "data_value": "double",
        "n": "int",
        "standard_error": "double",
        "lower_ci_bound": "double",
        "upper_ci_bound": "double",
    }

    existing_numeric_columns = {
        column: data_type
        for column, data_type in numeric_columns.items()
        if column in selected.columns
    }

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map=existing_numeric_columns,
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "onet_soc_code",
                "element_id",
                "scale_id",
                "category",
            ],
        )
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "element_id",
            "scale_id",
            "category",
            "data_value",
        ],
        key_columns=[
            "onet_soc_code",
            "element_id",
            "scale_id",
            "category",
        ],
        extra_issue_frames=[cast_issues],
    )


def build_onet_education(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET occupation education-rating table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean education ratings and associated quality issues.
    """
    return _build_category_rating_table(
        spark=spark,
        run_id=run_id,
        file_name="Education.txt",
        dataset_name="onet_education",
    )


def build_onet_training_experience(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET training-and-experience rating table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean training and experience ratings and quality issues.
    """
    return _build_category_rating_table(
        spark=spark,
        run_id=run_id,
        file_name="Training and Experience.txt",
        dataset_name="onet_training_experience",
    )


def build_onet_essential_skills(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Essential Skills rating table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean Essential Skills ratings and quality issues.
    """
    return _build_rating_table(
        spark=spark,
        run_id=run_id,
        file_name="Essential Skills.txt",
        dataset_name="onet_essential_skills",
    )


def build_onet_transferable_skills(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Transferable Skills rating table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean Transferable Skills ratings and quality issues.
    """
    return _build_rating_table(
        spark=spark,
        run_id=run_id,
        file_name="Transferable Skills.txt",
        dataset_name="onet_transferable_skills",
    )


def build_onet_knowledge(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Knowledge rating table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean knowledge ratings and associated quality issues.
    """
    return _build_rating_table(
        spark=spark,
        run_id=run_id,
        file_name="Knowledge.txt",
        dataset_name="onet_knowledge",
    )


def build_onet_abilities(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Abilities rating table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean ability ratings and associated quality issues.
    """
    return _build_rating_table(
        spark=spark,
        run_id=run_id,
        file_name="Abilities.txt",
        dataset_name="onet_abilities",
    )


def build_onet_work_activities(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Work Activities rating table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean work-activity ratings and quality issues.
    """
    return _build_rating_table(
        spark=spark,
        run_id=run_id,
        file_name="Work Activities.txt",
        dataset_name="onet_work_activities",
    )


def build_onet_software_skills(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Software Skills table.

    The table links occupations to workplace software examples and their
    O*NET content-model classifications. Hot Technology and In Demand
    fields are converted to Boolean values.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean software-skill records and associated quality issues.
    """
    dataset_name = "onet_software_skills"

    df, path = _base_onet_frame(
        spark=spark,
        file_name="Software Skills.txt",
        run_id=run_id,
    )

    df = _add_onet_soc_fields(df)

    df = (
        df.withColumn(
            "hot_technology",
            yes_no_to_boolean_udf(
                F.col("hot_technology")
            ),
        )
        .withColumn(
            "in_demand",
            yes_no_to_boolean_udf(
                F.col("in_demand")
            ),
        )
    )

    selected = df.select(
        F.col("onet_soc_code_raw"),
        F.col("onet_soc_code"),
        F.col("soc_code"),
        F.col("workplace_example").alias(
            "software_name"
        ),
        F.col("element_id"),
        F.col("element_name"),
        F.col("hot_technology"),
        F.col("in_demand"),
        F.col("source_file_path"),
        F.col("run_id"),
        F.col("processed_at"),
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "software_name",
            "element_id",
        ],
        key_columns=[
            "onet_soc_code",
            "software_name",
            "element_id",
        ],
    )


def build_onet_task_statements(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Task Statements table.

    Task identifiers and incumbent counts are cast to numeric types. Source
    dates are converted into monthly Spark date values.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean task statements and associated quality issues.
    """
    dataset_name = "onet_task_statements"

    df, path = _base_onet_frame(
        spark=spark,
        file_name="Task Statements.txt",
        run_id=run_id,
    )

    df = _add_onet_soc_fields(df)
    df = _add_source_date(df)

    selected = _select_existing_columns(
        df=df,
        columns=[
            "onet_soc_code_raw",
            "onet_soc_code",
            "soc_code",
            "task_id",
            "task",
            "task_type",
            "incumbents_responding",
            "source_date_raw",
            "data_month",
            "domain_source",
            "source_file_path",
            "run_id",
            "processed_at",
        ],
    )

    selected = selected.withColumnRenamed(
        "task",
        "task_statement",
    )

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map={
                "task_id": "long",
                "incumbents_responding": "int",
            },
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "onet_soc_code",
                "task_id",
            ],
        )
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "task_id",
            "task_statement",
        ],
        key_columns=[
            "onet_soc_code",
            "task_id",
        ],
        extra_issue_frames=[cast_issues],
    )


def build_onet_job_zones(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Job Zones table.

    Job Zone values are converted to integers and validated against the
    expected range of one through five.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean job-zone records and associated quality issues.
    """
    dataset_name = "onet_job_zones"

    df, path = _base_onet_frame(
        spark=spark,
        file_name="Job Zones.txt",
        run_id=run_id,
    )

    df = _add_onet_soc_fields(df)
    df = _add_source_date(df)

    selected = _select_existing_columns(
        df=df,
        columns=[
            "onet_soc_code_raw",
            "onet_soc_code",
            "soc_code",
            "job_zone",
            "source_date_raw",
            "data_month",
            "domain_source",
            "source_file_path",
            "run_id",
            "processed_at",
        ],
    )

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map={
                "job_zone": "int",
            },
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=["onet_soc_code"],
        )
    )

    range_issues = (
        selected.filter(
            F.col("job_zone").isNotNull()
            & ~F.col("job_zone").between(1, 5)
        )
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit("O*NET").alias(
                "source_system"
            ),
            F.lit(dataset_name).alias(
                "dataset_name"
            ),
            F.lit(path).alias(
                "source_file_path"
            ),
            F.col("onet_soc_code").alias(
                "record_key"
            ),
            F.lit("error").alias("severity"),
            F.lit("invalid_job_zone").alias(
                "rule_name"
            ),
            F.lit(
                "Job Zone must be between 1 and 5."
            ).alias("issue_message"),
            F.col("job_zone").cast(
                "string"
            ).alias("raw_value"),
            F.current_timestamp().alias(
                "detected_at"
            ),
        )
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "job_zone",
        ],
        key_columns=["onet_soc_code"],
        extra_issue_frames=[
            cast_issues,
            range_issues,
        ],
    )


def build_onet_related_occupations(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET Related Occupations relationship table.

    The function creates detailed and base SOC fields for both the source
    occupation and its related occupation. Relationship order is converted
    to an integer.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean occupation relationships and associated quality issues.
    """
    dataset_name = "onet_related_occupations"

    df, path = _base_onet_frame(
        spark=spark,
        file_name="Related Occupations.txt",
        run_id=run_id,
    )

    df = _add_onet_soc_fields(df)

    selected = df.select(
        F.col("onet_soc_code_raw"),
        F.col("onet_soc_code"),
        F.col("soc_code"),
        F.col("related_onet_soc_code").alias(
            "related_onet_soc_code_raw"
        ),
        F.col("relatedness_tier"),
        F.col("index"),
        F.col("source_file_path"),
        F.col("run_id"),
        F.col("processed_at"),
    )

    selected = (
        selected.withColumn(
            "related_onet_soc_code",
            normalize_onet_soc_code_udf(
                F.col(
                    "related_onet_soc_code_raw"
                )
            ),
        )
        .withColumn(
            "related_soc_code",
            normalize_soc_code_udf(
                F.col(
                    "related_onet_soc_code_raw"
                )
            ),
        )
    )

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map={
                "index": "int",
            },
            source_system="O*NET",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "onet_soc_code",
                "related_onet_soc_code",
            ],
        )
    )

    related_format_issues = invalid_regex_issues(
        df=selected,
        column="related_onet_soc_code_raw",
        pattern=r"^\d{2}-\d{4}\.\d{2}$",
        source_system="O*NET",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        key_columns=[
            "onet_soc_code",
            "related_onet_soc_code_raw",
        ],
    )

    return _finish_onet_table(
        df=selected,
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "related_onet_soc_code",
            "related_soc_code",
        ],
        key_columns=[
            "onet_soc_code",
            "related_onet_soc_code",
        ],
        extra_issue_frames=[
            cast_issues,
            related_format_issues,
        ],
    )


def build_onet_tables(
    spark: SparkSession,
    run_id: str,
) -> Tuple[Dict[str, DataFrame], DataFrame]:
    """
    Build every configured O*NET Silver DataFrame.

    Each builder returns a clean DataFrame and its quality issues. This
    function collects the tables into a dictionary and combines every O*NET
    issue into one DataFrame for insertion into data_quality_issues.

    Args:
        spark:
            Active Spark session.

        run_id:
            Unique identifier for the current Silver pipeline run.

    Returns:
        Tuple[Dict[str, DataFrame], DataFrame]:
            Mapping of Silver table names to clean DataFrames, followed by
            the combined O*NET data-quality issue DataFrame.
    """
    builders = {
        "onet_occupations": build_onet_occupations,
        "onet_education_categories": (
            build_onet_education_categories
        ),
        "onet_education": build_onet_education,
        "onet_training_experience": (
            build_onet_training_experience
        ),
        "onet_essential_skills": (
            build_onet_essential_skills
        ),
        "onet_transferable_skills": (
            build_onet_transferable_skills
        ),
        "onet_software_skills": (
            build_onet_software_skills
        ),
        "onet_knowledge": build_onet_knowledge,
        "onet_abilities": build_onet_abilities,
        "onet_work_activities": (
            build_onet_work_activities
        ),
        "onet_task_statements": (
            build_onet_task_statements
        ),
        "onet_job_zones": build_onet_job_zones,
        "onet_related_occupations": (
            build_onet_related_occupations
        ),
    }

    tables: Dict[str, DataFrame] = {}
    issue_frames: List[DataFrame] = []

    for table_name, builder in builders.items():
        print(f"Building {table_name}...")

        table_df, issue_df = builder(
            spark,
            run_id,
        )

        tables[table_name] = table_df
        issue_frames.append(issue_df)

    all_issues = union_issue_frames(
        spark=spark,
        frames=issue_frames,
    )

    return tables, all_issues