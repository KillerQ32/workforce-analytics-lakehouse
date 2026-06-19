"""
BLS Silver-layer table transformations.

This module reads raw BLS Bronze files and creates cleaned, validated,
deduplicated Spark DataFrames for:

- OEWS observations
- OEWS series metadata
- OEWS occupations
- OEWS areas
- OEWS industries
- OEWS datatypes
- OEWS footnotes
- Employment Projections
- National Employment Matrix
- O*NET-SOC crosswalks
- NEM occupational coverage

The module creates source-aligned Silver tables. Analytics-ready joins and
views belong in the Gold layer.
"""

from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils import (
    add_metadata,
    add_soc_columns,
    cast_columns_with_issues,
    deduplicate_with_issues,
    filter_valid_required_rows,
    invalid_regex_issues,
    normalize_columns,
    normalize_onet_soc_code_udf,
    normalize_soc_code_udf,
    nullify_placeholders,
    read_delimited_file,
    read_excel_sheet_as_spark,
    required_value_issues,
    trim_string_columns,
    union_issue_frames,
)


BLS_BRONZE_BASE = (
    "/Volumes/workforce_analytics/bronze/raw_files/bls"
)

OEWS_BASE = f"{BLS_BRONZE_BASE}/oews"
EP_BASE = f"{BLS_BRONZE_BASE}/employment_projections"
CROSSWALK_BASE = f"{BLS_BRONZE_BASE}/crosswalks"

EMPLOYMENT_PROJECTIONS_FILE = (
    f"{EP_BASE}/"
    "employment_projections_occupation_tables_2024_2034.xlsx"
)

NATIONAL_EMPLOYMENT_MATRIX_FILE = (
    f"{EP_BASE}/"
    "national_employment_matrix_2024_2034.xlsx"
)

ONET_SOC_CROSSWALK_FILE = (
    f"{CROSSWALK_BASE}/onet_soc_to_nem_crosswalk.xlsx"
)

NEM_OCCUPATIONAL_COVERAGE_FILE = (
    f"{CROSSWALK_BASE}/nem_occupational_coverage.xlsx"
)


def _base_text_frame(
    spark: SparkSession,
    path: str,
    run_id: str,
) -> DataFrame:
    """
    Read and apply base cleaning to one BLS tab-delimited text file.

    Args:
        spark:
            Active Spark session.

        path:
            Bronze source file path.

        run_id:
            Current Silver pipeline run identifier.

    Returns:
        DataFrame:
            Base-cleaned Spark DataFrame.
    """
    df = read_delimited_file(
        spark=spark,
        path=path,
        delimiter="\t",
    )

    df = normalize_columns(df)
    df = trim_string_columns(df)
    df = nullify_placeholders(df)
    df = add_metadata(df, path, run_id)

    return df


def _base_excel_frame(
    spark: SparkSession,
    path: str,
    sheet_name: str,
    header_row: int,
    run_id: str,
) -> DataFrame:
    """
    Read and apply base cleaning to one BLS Excel worksheet.

    Args:
        spark:
            Active Spark session.

        path:
            Bronze Excel workbook path.

        sheet_name:
            Worksheet name.

        header_row:
            Zero-based row containing headers.

        run_id:
            Current Silver pipeline run identifier.

    Returns:
        DataFrame:
            Base-cleaned Spark DataFrame.
    """
    df = read_excel_sheet_as_spark(
        spark=spark,
        path=path,
        sheet_name=sheet_name,
        header_row=header_row,
    )

    df = normalize_columns(df)
    df = trim_string_columns(df)
    df = nullify_placeholders(df)
    df = add_metadata(df, path, run_id)

    return df


def _find_column(
    df: DataFrame,
    candidates: Sequence[str],
    target_name: str,
    required: bool = True,
) -> Optional[str]:
    """
    Find the first available source column from a list of candidates.

    BLS occasionally changes punctuation, wording, or footnote suffixes in
    Excel headers. This helper allows several normalized header variations.

    Args:
        df:
            DataFrame whose columns will be searched.

        candidates:
            Ordered possible source column names.

        target_name:
            Stable Silver field being created.

        required:
            Whether absence should stop the transformation.

    Returns:
        str or None:
            Matching source column name, or None for an optional field.

    Raises:
        ValueError:
            If no required candidate exists.
    """
    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    if required:
        raise ValueError(
            f"Could not create {target_name!r}. None of the expected "
            f"source columns were found: {list(candidates)}. "
            f"Available columns: {df.columns}"
        )

    return None


def _select_mapped_columns(
    df: DataFrame,
    required_mapping: Mapping[
        str,
        Sequence[str],
    ],
    optional_mapping: Optional[
        Mapping[str, Sequence[str]]
    ] = None,
) -> DataFrame:
    """
    Select source fields and rename them to stable Silver column names.

    Args:
        df:
            Source DataFrame.

        required_mapping:
            Mapping from target names to possible required source names.

        optional_mapping:
            Mapping from target names to possible optional source names.

    Returns:
        DataFrame:
            DataFrame containing mapped columns and lineage metadata.
    """
    expressions = []

    for target_name, candidates in required_mapping.items():
        source_name = _find_column(
            df=df,
            candidates=candidates,
            target_name=target_name,
            required=True,
        )

        expressions.append(
            F.col(source_name).alias(
                target_name
            )
        )

    for target_name, candidates in (
        optional_mapping or {}
    ).items():
        source_name = _find_column(
            df=df,
            candidates=candidates,
            target_name=target_name,
            required=False,
        )

        if source_name:
            expressions.append(
                F.col(source_name).alias(
                    target_name
                )
            )

    for metadata_column in (
        "source_file_path",
        "run_id",
        "processed_at",
    ):
        if metadata_column in df.columns:
            expressions.append(
                F.col(metadata_column)
            )

    return df.select(
        *expressions
    )


def _finish_table(
    df: DataFrame,
    required_columns: Sequence[str],
    key_columns: Sequence[str],
    source_system: str,
    dataset_name: str,
    source_file_path: str,
    run_id: str,
    extra_issue_frames: Optional[
        List[DataFrame]
    ] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Apply shared required-field validation and deduplication.

    Args:
        df:
            BLS DataFrame to validate.

        required_columns:
            Fields required for a valid Silver row.

        key_columns:
            Fields that identify a logical record.

        source_system:
            Source label.

        dataset_name:
            Target Silver dataset name.

        source_file_path:
            Bronze source path.

        run_id:
            Current pipeline run identifier.

        extra_issue_frames:
            Optional dataset-specific issue DataFrames.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean records and combined data-quality issues.
    """
    issue_frames: List[DataFrame] = list(
        extra_issue_frames or []
    )

    issue_frames.append(
        required_value_issues(
            df=df,
            required_columns=required_columns,
            source_system=source_system,
            dataset_name=dataset_name,
            source_file_path=source_file_path,
            run_id=run_id,
            key_columns=key_columns,
        )
    )

    valid_df = filter_valid_required_rows(
        df=df,
        required_columns=required_columns,
    )

    valid_df, duplicate_issues = (
        deduplicate_with_issues(
            df=valid_df,
            key_columns=key_columns,
            source_system=source_system,
            dataset_name=dataset_name,
            source_file_path=source_file_path,
            run_id=run_id,
        )
    )

    issue_frames.append(
        duplicate_issues
    )

    return (
        valid_df,
        union_issue_frames(
            spark=df.sparkSession,
            frames=issue_frames,
        ),
    )


def build_oews_observations(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the cleaned OEWS observation table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean observations and quality issues.
    """
    path = f"{OEWS_BASE}/oe_data_all_data.txt"
    dataset_name = "bls_oews_observations"

    df = _base_text_frame(
        spark=spark,
        path=path,
        run_id=run_id,
    )

    selected = _select_mapped_columns(
        df=df,
        required_mapping={
            "series_id": (
                "series_id",
            ),
            "year": (
                "year",
            ),
            "period": (
                "period",
            ),
            "value_raw": (
                "value",
            ),
        },
        optional_mapping={
            "footnote_codes": (
                "footnote_codes",
                "footnote_code",
            ),
        },
    )

    selected = selected.withColumn(
        "value",
        F.col("value_raw"),
    )

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map={
                "year": "int",
                "value": "double",
            },
            source_system="BLS",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "series_id",
                "year",
                "period",
            ],
        )
    )

    period_issues = invalid_regex_issues(
        df=selected,
        column="period",
        pattern=r"^(A01|M(0[1-9]|1[0-3])|S0[1-2])$",
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        key_columns=[
            "series_id",
            "year",
            "period",
        ],
    )

    return _finish_table(
        df=selected,
        required_columns=[
            "series_id",
            "year",
            "period",
        ],
        key_columns=[
            "series_id",
            "year",
            "period",
        ],
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=[
            cast_issues,
            period_issues,
        ],
    )


def build_oews_series(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the cleaned OEWS series metadata table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean series rows and quality issues.
    """
    path = f"{OEWS_BASE}/oe_series.txt"
    dataset_name = "bls_oews_series"

    df = _base_text_frame(
        spark=spark,
        path=path,
        run_id=run_id,
    )

    df = add_soc_columns(
        df=df,
        bls_column="occupation_code",
    )

    selected_columns = [
        "series_id",
        "seasonal",
        "areatype_code",
        "area_type_code",
        "state_code",
        "area_code",
        "industry_code",
        "occupation_code",
        "soc_code",
        "datatype_code",
        "sector_code",
        "series_title",
        "footnote_codes",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
        "source_file_path",
        "run_id",
        "processed_at",
    ]

    selected = df.select(
        *[
            F.col(column)
            for column in selected_columns
            if column in df.columns
        ]
    )

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map={
                "begin_year": "int",
                "end_year": "int",
            },
            source_system="BLS",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "series_id",
            ],
        )
    )

    return _finish_table(
        df=selected,
        required_columns=[
            "series_id",
            "occupation_code",
            "datatype_code",
        ],
        key_columns=[
            "series_id",
        ],
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=[
            cast_issues,
        ],
    )


def build_oews_occupations(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the cleaned OEWS occupation lookup table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean occupation rows and quality issues.
    """
    path = f"{OEWS_BASE}/oe_occupation.txt"
    dataset_name = "bls_oews_occupations"

    df = _base_text_frame(
        spark=spark,
        path=path,
        run_id=run_id,
    )

    df = add_soc_columns(
        df=df,
        bls_column="occupation_code",
    )

    selected_columns = [
        "occupation_code",
        "soc_code",
        "occupation_name",
        "occupation_description",
        "display_level",
        "selectable",
        "sort_sequence",
        "source_file_path",
        "run_id",
        "processed_at",
    ]

    selected = df.select(
        *[
            F.col(column)
            for column in selected_columns
            if column in df.columns
        ]
    )

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map={
                "display_level": "int",
                "sort_sequence": "int",
            },
            source_system="BLS",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "occupation_code",
            ],
        )
    )

    return _finish_table(
        df=selected,
        required_columns=[
            "occupation_code",
            "occupation_name",
        ],
        key_columns=[
            "occupation_code",
        ],
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=[
            cast_issues,
        ],
    )


def _build_simple_lookup(
    spark: SparkSession,
    run_id: str,
    file_name: str,
    dataset_name: str,
    required_columns: Sequence[str],
    key_columns: Sequence[str],
    cast_map: Optional[
        Dict[str, str]
    ] = None,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build a cleaned OEWS lookup table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

        file_name:
            OEWS Bronze filename.

        dataset_name:
            Target Silver table name.

        required_columns:
            Required lookup fields.

        key_columns:
            Lookup business key.

        cast_map:
            Optional field-to-type mapping.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean lookup rows and quality issues.
    """
    path = f"{OEWS_BASE}/{file_name}"

    df = _base_text_frame(
        spark=spark,
        path=path,
        run_id=run_id,
    )

    issue_frames: List[DataFrame] = []

    if cast_map:
        df, cast_issues = (
            cast_columns_with_issues(
                df=df,
                cast_map=cast_map,
                source_system="BLS",
                dataset_name=dataset_name,
                source_file_path=path,
                run_id=run_id,
                key_columns=key_columns,
            )
        )

        issue_frames.append(
            cast_issues
        )

    return _finish_table(
        df=df,
        required_columns=required_columns,
        key_columns=key_columns,
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=issue_frames,
    )


def build_oews_areas(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the cleaned OEWS area lookup table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean area rows and quality issues.
    """
    return _build_simple_lookup(
        spark=spark,
        run_id=run_id,
        file_name="oe_area.txt",
        dataset_name="bls_oews_areas",
        required_columns=[
            "area_code",
            "area_name",
        ],
        key_columns=[
            "area_type_code",
            "areatype_code",
            "area_code",
        ],
    )


def build_oews_industries(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the cleaned OEWS industry lookup table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean industry rows and quality issues.
    """
    return _build_simple_lookup(
        spark=spark,
        run_id=run_id,
        file_name="oe_industry.txt",
        dataset_name="bls_oews_industries",
        required_columns=[
            "industry_code",
            "industry_name",
        ],
        key_columns=[
            "industry_code",
        ],
        cast_map={
            "display_level": "int",
            "sort_sequence": "int",
        },
    )


def build_oews_datatypes(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the cleaned OEWS datatype lookup table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean datatype rows and quality issues.
    """
    return _build_simple_lookup(
        spark=spark,
        run_id=run_id,
        file_name="oe_datatype.txt",
        dataset_name="bls_oews_datatypes",
        required_columns=[
            "datatype_code",
            "datatype_name",
        ],
        key_columns=[
            "datatype_code",
        ],
    )


def build_oews_footnotes(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the cleaned OEWS footnote lookup table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean footnote rows and quality issues.
    """
    return _build_simple_lookup(
        spark=spark,
        run_id=run_id,
        file_name="oe_footnote.txt",
        dataset_name="bls_oews_footnotes",
        required_columns=[
            "footnote_code",
            "footnote_text",
        ],
        key_columns=[
            "footnote_code",
        ],
    )


def build_employment_projections(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the occupation-level Employment Projections table.

    The BLS wage ceiling value is preserved with three fields:

    - median_annual_wage_raw
    - median_annual_wage
    - median_annual_wage_qualifier

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean projection rows and quality issues.
    """
    path = EMPLOYMENT_PROJECTIONS_FILE
    dataset_name = "bls_employment_projections"

    df = _base_excel_frame(
        spark=spark,
        path=path,
        sheet_name="Table 1.2",
        header_row=1,
        run_id=run_id,
    )

    selected = _select_mapped_columns(
        df=df,
        required_mapping={
            "occupation_title": (
                "2024_national_employment_matrix_title",
                "national_employment_matrix_title",
                "occupation_title",
            ),
            "soc_code_raw": (
                "2024_national_employment_matrix_code",
                "national_employment_matrix_code",
                "occupation_code",
                "soc_code",
            ),
            "employment_base_year_thousands": (
                "employment_2024",
                "2024_employment",
            ),
            "employment_projected_year_thousands": (
                "employment_2034",
                "2034_employment",
            ),
            "employment_change_thousands": (
                "employment_change_numeric_2024_34",
                "numeric_change_2024_34",
            ),
            "employment_change_percent": (
                "employment_change_percent_2024_34",
                "percent_change_2024_34",
            ),
            "annual_openings_thousands": (
                "occupational_openings_2024_34_annual_average",
                "annual_average_occupational_openings_2024_34",
            ),
        },
        optional_mapping={
            "occupation_type": (
                "occupation_type",
            ),
            "employment_distribution_base_percent": (
                "employment_distribution_percent_2024",
            ),
            "employment_distribution_projected_percent": (
                "employment_distribution_percent_2034",
            ),
            "percent_self_employed": (
                "percent_self_employed_2024",
            ),
            "median_annual_wage": (
                "median_annual_wage_dollars_2024_1",
                "median_annual_wage_dollars_2024",
                "median_annual_wage_2024",
            ),
            "typical_entry_education": (
                "typical_education_needed_for_entry",
            ),
            "related_work_experience": (
                "work_experience_in_a_related_occupation",
            ),
            "on_the_job_training": (
                "typical_on_the_job_training_needed_to_attain_competency_in_the_occupation",
                "typical_on_the_job_training",
            ),
        },
    )

    selected = selected.withColumn(
        "soc_code",
        normalize_soc_code_udf(
            F.col("soc_code_raw")
        ),
    )

    if "median_annual_wage" in selected.columns:
        selected = selected.withColumn(
            "median_annual_wage_raw",
            F.col("median_annual_wage"),
        )

        compact_wage = F.regexp_replace(
            F.trim(
                F.col("median_annual_wage_raw")
            ),
            r"\s+",
            "",
        )

        selected = selected.withColumn(
            "median_annual_wage_qualifier",
            F.when(
                F.col(
                    "median_annual_wage_raw"
                ).isNull(),
                F.lit(None).cast("string"),
            )
            .when(
                compact_wage.rlike(
                    r"^(>=|≥)"
                ),
                F.lit(
                    "greater_than_or_equal"
                ),
            )
            .when(
                compact_wage.rlike(
                    r"^(<=|≤)"
                ),
                F.lit(
                    "less_than_or_equal"
                ),
            )
            .when(
                compact_wage.rlike(
                    r"^>"
                ),
                F.lit("greater_than"),
            )
            .when(
                compact_wage.rlike(
                    r"^<"
                ),
                F.lit("less_than"),
            )
            .otherwise(
                F.lit("exact")
            ),
        )

        selected = selected.withColumn(
            "median_annual_wage",
            F.regexp_replace(
                compact_wage,
                r"^(>=|<=|>|<|≥|≤)",
                "",
            ),
        )

        selected = selected.withColumn(
            "median_annual_wage",
            F.regexp_replace(
                F.col(
                    "median_annual_wage"
                ),
                r"[$,]",
                "",
            ),
        )

    numeric_columns = {
        "employment_base_year_thousands": "double",
        "employment_projected_year_thousands": "double",
        "employment_change_thousands": "double",
        "employment_change_percent": "double",
        "annual_openings_thousands": "double",
        "employment_distribution_base_percent": "double",
        "employment_distribution_projected_percent": "double",
        "percent_self_employed": "double",
        "median_annual_wage": "double",
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
            source_system="BLS",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "soc_code_raw",
            ],
        )
    )

    soc_issues = invalid_regex_issues(
        df=selected,
        column="soc_code_raw",
        pattern=r"^\d{2}-\d{4}$",
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        key_columns=[
            "soc_code_raw",
        ],
    )

    return _finish_table(
        df=selected,
        required_columns=[
            "soc_code",
            "occupation_title",
        ],
        key_columns=[
            "soc_code",
            "occupation_type",
        ],
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=[
            cast_issues,
            soc_issues,
        ],
    )


def build_national_employment_matrix(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the National Employment Matrix table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean matrix rows and quality issues.
    """
    path = NATIONAL_EMPLOYMENT_MATRIX_FILE
    dataset_name = "bls_national_employment_matrix"

    df = _base_excel_frame(
        spark=spark,
        path=path,
        sheet_name="Matrix",
        header_row=0,
        run_id=run_id,
    )

    selected = _select_mapped_columns(
        df=df,
        required_mapping={
            "soc_code_raw": (
                "occupation_code",
                "national_employment_matrix_code",
                "soc_code",
            ),
            "occupation_title": (
                "occupation_title",
                "national_employment_matrix_title",
            ),
            "industry_code": (
                "industry_code",
            ),
            "industry_title": (
                "industry_title",
            ),
            "employment_base_year_thousands": (
                "2024_employment",
                "employment_2024",
            ),
            "employment_projected_year_thousands": (
                "2034_employment",
                "employment_2034",
            ),
        },
        optional_mapping={
            "occupation_type": (
                "occupation_type",
            ),
            "industry_type": (
                "industry_type",
            ),
            "base_percent_of_industry": (
                "2024_percent_of_industry",
            ),
            "base_percent_of_occupation": (
                "2024_percent_of_occupation",
            ),
            "projected_percent_of_industry": (
                "2034_percent_of_industry",
            ),
            "projected_percent_of_occupation": (
                "2034_percent_of_occupation",
            ),
            "employment_change_thousands": (
                "numeric_change_2024_34",
                "employment_change_numeric_2024_34",
            ),
            "employment_change_percent": (
                "percent_change_2024_34",
                "employment_change_percent_2024_34",
            ),
        },
    )

    selected = selected.withColumn(
        "soc_code",
        normalize_soc_code_udf(
            F.col("soc_code_raw")
        ),
    )

    numeric_columns = {
        "employment_base_year_thousands": "double",
        "employment_projected_year_thousands": "double",
        "base_percent_of_industry": "double",
        "base_percent_of_occupation": "double",
        "projected_percent_of_industry": "double",
        "projected_percent_of_occupation": "double",
        "employment_change_thousands": "double",
        "employment_change_percent": "double",
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
            source_system="BLS",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "soc_code_raw",
                "industry_code",
            ],
        )
    )

    soc_issues = invalid_regex_issues(
        df=selected,
        column="soc_code_raw",
        pattern=r"^\d{2}-\d{4}$",
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        key_columns=[
            "soc_code_raw",
            "industry_code",
        ],
    )

    return _finish_table(
        df=selected,
        required_columns=[
            "soc_code",
            "industry_code",
            "occupation_title",
            "industry_title",
        ],
        key_columns=[
            "soc_code",
            "industry_code",
            "occupation_type",
            "industry_type",
        ],
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=[
            cast_issues,
            soc_issues,
        ],
    )


def build_onet_soc_crosswalk(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the O*NET-SOC to BLS/NEM crosswalk table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean crosswalk rows and quality issues.
    """
    path = ONET_SOC_CROSSWALK_FILE
    dataset_name = "bls_onet_soc_crosswalk"

    df = _base_excel_frame(
        spark=spark,
        path=path,
        sheet_name="ONET to SOC crosswalk",
        header_row=4,
        run_id=run_id,
    )

    selected = _select_mapped_columns(
        df=df,
        required_mapping={
            "onet_soc_code_raw": (
                "onet_soc_code",
                "o_net_soc_code",
            ),
            "onet_title": (
                "onet_soc_title",
                "o_net_soc_title",
                "onet_title",
            ),
            "soc_code_raw": (
                "nem_code",
                "national_employment_matrix_code",
                "soc_code",
            ),
            "bls_title": (
                "nem_title",
                "national_employment_matrix_title",
                "bls_title",
            ),
        },
        optional_mapping={
            "sort_order": (
                "sort_order",
            ),
            "ooh_profile_code": (
                "ooh_profile_code",
            ),
            "ooh_profile_title": (
                "ooh_profile_title",
            ),
            "ooh_occupation_group": (
                "ooh_occupation_group",
            ),
            "ooh_occupation_group_brief": (
                "ooh_occupation_group_brief",
            ),
            "ooh_profile_website": (
                "ooh_profile_website",
            ),
        },
    )

    selected = (
        selected.withColumn(
            "onet_soc_code",
            normalize_onet_soc_code_udf(
                F.col(
                    "onet_soc_code_raw"
                )
            ),
        )
        .withColumn(
            "soc_code",
            normalize_soc_code_udf(
                F.col("soc_code_raw")
            ),
        )
    )

    if "sort_order" in selected.columns:
        selected, sort_issues = (
            cast_columns_with_issues(
                df=selected,
                cast_map={
                    "sort_order": "int",
                },
                source_system="BLS",
                dataset_name=dataset_name,
                source_file_path=path,
                run_id=run_id,
                key_columns=[
                    "onet_soc_code_raw",
                    "soc_code_raw",
                ],
            )
        )

    else:
        sort_issues = union_issue_frames(
            spark=spark,
            frames=[],
        )

    onet_format_issues = invalid_regex_issues(
        df=selected,
        column="onet_soc_code_raw",
        pattern=r"^\d{2}-\d{4}\.\d{2}$",
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        key_columns=[
            "onet_soc_code_raw",
            "soc_code_raw",
        ],
    )

    soc_format_issues = invalid_regex_issues(
        df=selected,
        column="soc_code_raw",
        pattern=r"^\d{2}-\d{4}$",
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        key_columns=[
            "onet_soc_code_raw",
            "soc_code_raw",
        ],
    )

    return _finish_table(
        df=selected,
        required_columns=[
            "onet_soc_code",
            "soc_code",
            "onet_title",
            "bls_title",
        ],
        key_columns=[
            "onet_soc_code",
            "soc_code",
        ],
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=[
            sort_issues,
            onet_format_issues,
            soc_format_issues,
        ],
    )


def build_nem_occupational_coverage(
    spark: SparkSession,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Build the NEM occupational coverage reference table.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[DataFrame, DataFrame]:
            Clean coverage rows and quality issues.
    """
    path = NEM_OCCUPATIONAL_COVERAGE_FILE
    dataset_name = "bls_nem_occupational_coverage"

    df = _base_excel_frame(
        spark=spark,
        path=path,
        sheet_name="2024-34 Occupational Directory",
        header_row=3,
        run_id=run_id,
    )

    selected = _select_mapped_columns(
        df=df,
        required_mapping={
            "soc_code_raw": (
                "national_employment_matrix_code",
                "nem_code",
                "occupation_code",
            ),
            "occupation_title": (
                "national_employment_matrix_title",
                "nem_title",
                "occupation_title",
            ),
        },
        optional_mapping={
            "sort_order": (
                "sort_order",
            ),
            "occupation_type": (
                "occupation_type",
            ),
            "occupation_level": (
                "level",
                "occupation_level",
            ),
        },
    )

    selected = selected.withColumn(
        "soc_code",
        normalize_soc_code_udf(
            F.col("soc_code_raw")
        ),
    )

    cast_map = {
        column: "int"
        for column in (
            "sort_order",
            "occupation_level",
        )
        if column in selected.columns
    }

    selected, cast_issues = (
        cast_columns_with_issues(
            df=selected,
            cast_map=cast_map,
            source_system="BLS",
            dataset_name=dataset_name,
            source_file_path=path,
            run_id=run_id,
            key_columns=[
                "soc_code_raw",
            ],
        )
    )

    soc_issues = invalid_regex_issues(
        df=selected,
        column="soc_code_raw",
        pattern=r"^\d{2}-\d{4}$",
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        key_columns=[
            "soc_code_raw",
        ],
    )

    return _finish_table(
        df=selected,
        required_columns=[
            "soc_code",
            "occupation_title",
        ],
        key_columns=[
            "soc_code",
            "occupation_type",
        ],
        source_system="BLS",
        dataset_name=dataset_name,
        source_file_path=path,
        run_id=run_id,
        extra_issue_frames=[
            cast_issues,
            soc_issues,
        ],
    )


def build_bls_tables(
    spark: SparkSession,
    run_id: str,
) -> Tuple[Dict[str, DataFrame], DataFrame]:
    """
    Build every configured BLS Silver DataFrame.

    Args:
        spark:
            Active Spark session.

        run_id:
            Current pipeline run identifier.

    Returns:
        Tuple[Dict[str, DataFrame], DataFrame]:
            Mapping of table names to clean DataFrames and combined issues.
    """
    builders = {
        "bls_oews_observations": (
            build_oews_observations
        ),
        "bls_oews_series": (
            build_oews_series
        ),
        "bls_oews_occupations": (
            build_oews_occupations
        ),
        "bls_oews_areas": (
            build_oews_areas
        ),
        "bls_oews_industries": (
            build_oews_industries
        ),
        "bls_oews_datatypes": (
            build_oews_datatypes
        ),
        "bls_oews_footnotes": (
            build_oews_footnotes
        ),
        "bls_employment_projections": (
            build_employment_projections
        ),
        "bls_national_employment_matrix": (
            build_national_employment_matrix
        ),
        "bls_onet_soc_crosswalk": (
            build_onet_soc_crosswalk
        ),
        "bls_nem_occupational_coverage": (
            build_nem_occupational_coverage
        ),
    }

    tables: Dict[str, DataFrame] = {}
    issue_frames: List[DataFrame] = []

    for table_name, builder in builders.items():
        print(
            f"Building {table_name}..."
        )

        table_df, issue_df = builder(
            spark,
            run_id,
        )

        tables[table_name] = table_df
        issue_frames.append(
            issue_df
        )

    return (
        tables,
        union_issue_frames(
            spark=spark,
            frames=issue_frames,
        ),
    )