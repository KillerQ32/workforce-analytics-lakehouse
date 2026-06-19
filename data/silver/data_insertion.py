"""
Silver-layer pipeline entry point and Delta table insertion.

This module runs the complete Silver pipeline by:

1. Retrieving the Databricks Spark session
2. Creating the Unity Catalog Silver schema when needed
3. Building cleaned BLS DataFrames
4. Writing BLS managed Delta tables
5. Building cleaned O*NET DataFrames
6. Writing O*NET managed Delta tables
7. Appending validation issues
8. Appending processing-audit records

Run this file as the Databricks Python job task:

    src/silver/data_insertion.py
"""

import traceback
import uuid

from datetime import datetime, timezone
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from bls_tables import build_bls_tables
from onet_tables import build_onet_tables

from utils import (
    CATALOG_NAME,
    SILVER_SCHEMA,
    ensure_silver_schema,
    get_spark,
    union_issue_frames,
    write_delta_table,
)

TABLE_WRITE_MODE = "overwrite"

DATA_QUALITY_TABLE = (
    f"{CATALOG_NAME}."
    f"{SILVER_SCHEMA}."
    "data_quality_issues"
)

PROCESSING_AUDIT_TABLE = (
    f"{CATALOG_NAME}."
    f"{SILVER_SCHEMA}."
    "processing_audit"
)


AUDIT_SCHEMA = T.StructType(
    [
        T.StructField(
            "run_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "source_system",
            T.StringType(),
            False,
        ),
        T.StructField(
            "table_name",
            T.StringType(),
            False,
        ),
        T.StructField(
            "status",
            T.StringType(),
            False,
        ),
        T.StructField(
            "row_count",
            T.LongType(),
            True,
        ),
        T.StructField(
            "write_mode",
            T.StringType(),
            False,
        ),
        T.StructField(
            "error_message",
            T.StringType(),
            True,
        ),
        T.StructField(
            "started_at",
            T.TimestampType(),
            False,
        ),
        T.StructField(
            "completed_at",
            T.TimestampType(),
            False,
        ),
    ]
)


def _utc_now() -> datetime:
    """
    Return the current UTC time without timezone metadata.

    Spark TimestampType stores the timestamp value without the Python
    timezone object. This helper creates a consistent timestamp for
    processing-audit records.

    Returns:
        datetime:
            Current timezone-naive UTC timestamp.
    """
    return datetime.now(
        timezone.utc
    ).replace(
        tzinfo=None
    )


def write_source_tables(
    source_system: str,
    tables: Dict[str, DataFrame],
    run_id: str,
    audit_rows: List[Tuple],
) -> None:
    """
    Write cleaned source DataFrames as managed Silver Delta tables.

    Each DataFrame is written directly without Spark caching because
    DataFrame persist, cache, and unpersist operations are unsupported on
    Databricks Serverless compute.

    After each write, the function reads the completed Delta table to obtain
    its stored row count and records the result in the processing audit.

    Args:
        source_system:
            Source label such as BLS or O*NET.

        tables:
            Mapping of target Silver table names to Spark DataFrames.

        run_id:
            Unique identifier for the current Silver pipeline run.

        audit_rows:
            Mutable list used to collect processing-audit records.

    Returns:
        None.

    Raises:
        Exception:
            Re-raises a Delta write failure so the pipeline stops.
    """
    for short_name, df in tables.items():
        full_table_name = (
            f"{CATALOG_NAME}."
            f"{SILVER_SCHEMA}."
            f"{short_name}"
        )

        started_at = _utc_now()

        try:
            print(f"Writing {full_table_name}...")

            write_delta_table(
                df=df,
                table_name=full_table_name,
                mode=TABLE_WRITE_MODE,
            )

            # Count the records from the completed Delta table.
            row_count = (
                df.sparkSession
                .table(full_table_name)
                .count()
            )

            completed_at = _utc_now()

            audit_rows.append(
                (
                    run_id,
                    source_system,
                    full_table_name,
                    "success",
                    row_count,
                    TABLE_WRITE_MODE,
                    None,
                    started_at,
                    completed_at,
                )
            )

            print(
                f"Created {full_table_name} "
                f"with {row_count:,} rows."
            )

        except Exception as error:
            completed_at = _utc_now()

            error_message = (
                f"{type(error).__name__}: {error}"
            )

            audit_rows.append(
                (
                    run_id,
                    source_system,
                    full_table_name,
                    "failed",
                    None,
                    TABLE_WRITE_MODE,
                    error_message,
                    started_at,
                    completed_at,
                )
            )

            print(
                f"Failed to write "
                f"{full_table_name}: "
                f"{error_message}"
            )

            raise


def write_data_quality_issues(
    spark: SparkSession,
    issue_frames: List[DataFrame],
) -> int:
    """
    Combine and append validation issues to the Silver quality table.

    The function avoids DataFrame caching because persist and unpersist
    operations are unsupported on Databricks Serverless compute.

    Args:
        spark:
            Active Spark session.

        issue_frames:
            BLS and O*NET issue DataFrames generated during the pipeline.

    Returns:
        int:
            Number of issue records appended during the current run.
    """
    all_issues = union_issue_frames(
        spark=spark,
        frames=issue_frames,
    )

    issue_count = all_issues.count()

    write_delta_table(
        df=all_issues,
        table_name=DATA_QUALITY_TABLE,
        mode="append",
    )

    return issue_count


def write_audit_rows(
    spark: SparkSession,
    audit_rows: List[Tuple],
) -> None:
    """
    Append processing-audit records to the Silver audit Delta table.

    The function does nothing when the current run has not yet generated
    any audit records.

    Args:
        spark:
            Active Spark session.

        audit_rows:
            Collection of table processing results for the current run.

    Returns:
        None.
    """
    if not audit_rows:
        return

    audit_df = spark.createDataFrame(
        audit_rows,
        AUDIT_SCHEMA,
    )

    write_delta_table(
        df=audit_df,
        table_name=PROCESSING_AUDIT_TABLE,
        mode="append",
    )


def main() -> None:
    """
    Run the complete Workforce Analytics Silver pipeline.

    The function builds the BLS and O*NET source-aligned tables, writes them
    as managed Delta tables, appends validation issues, and records table
    processing results.

    Each pipeline execution receives a unique run_id for traceability.

    Returns:
        None.

    Raises:
        RuntimeError:
            Raised when any transformation or Delta table write fails.
    """
    spark = get_spark()

    ensure_silver_schema(
        spark=spark,
    )

    run_id = str(
        uuid.uuid4()
    )

    audit_rows: List[Tuple] = []
    issue_frames: List[DataFrame] = []

    pipeline_started_at = _utc_now()

    print(
        f"Starting Silver pipeline. "
        f"run_id={run_id}"
    )

    try:
        print(
            "Building BLS Silver tables..."
        )

        bls_tables, bls_issues = (
            build_bls_tables(
                spark=spark,
                run_id=run_id,
            )
        )

        issue_frames.append(
            bls_issues
        )

        write_source_tables(
            source_system="BLS",
            tables=bls_tables,
            run_id=run_id,
            audit_rows=audit_rows,
        )

        print(
            "Building O*NET Silver tables..."
        )

        onet_tables, onet_issues = (
            build_onet_tables(
                spark=spark,
                run_id=run_id,
            )
        )

        issue_frames.append(
            onet_issues
        )

        write_source_tables(
            source_system="O*NET",
            tables=onet_tables,
            run_id=run_id,
            audit_rows=audit_rows,
        )

        issue_count = write_data_quality_issues(
            spark=spark,
            issue_frames=issue_frames,
        )

        print(
            f"Appended {issue_count:,} "
            f"validation issues to "
            f"{DATA_QUALITY_TABLE}."
        )

        write_audit_rows(
            spark=spark,
            audit_rows=audit_rows,
        )

        duration_seconds = (
            _utc_now() - pipeline_started_at
        ).total_seconds()

        print(
            "Silver pipeline completed "
            f"successfully in "
            f"{duration_seconds:.1f} seconds."
        )

    except Exception as error:
        print(
            "Silver pipeline failed."
        )

        print(
            traceback.format_exc()
        )

        try:
            write_audit_rows(
                spark=spark,
                audit_rows=audit_rows,
            )

        except Exception as audit_error:
            print(
                "Could not write the processing "
                f"audit table: {audit_error}"
            )

        raise RuntimeError(
            "Silver pipeline failed for "
            f"run_id={run_id}: {error}"
        ) from error


if __name__ == "__main__":
    main()