import json
import time
from pathlib import Path
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BRONZE_BLS_PATH = Path("/Volumes/workforce_analytics/bronze/raw_files/bls")

MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 5


BLS_CORE_FILES = [
    # -----------------------------
    # OEWS bulk text files
    # -----------------------------
    {
        "source_group": "oews",
        "file_name": "oe_data_all_data.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.data.1.AllData",
        "description": "OEWS all employment and wage time-series data",
        "required": True,
    },
    {
        "source_group": "oews",
        "file_name": "oe_series.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.series",
        "description": "OEWS series metadata used to decode series IDs",
        "required": True,
    },
    {
        "source_group": "oews",
        "file_name": "oe_occupation.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.occupation",
        "description": "OEWS occupation lookup table with SOC occupation codes and titles",
        "required": True,
    },
    {
        "source_group": "oews",
        "file_name": "oe_area.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.area",
        "description": "OEWS area lookup table",
        "required": True,
    },
    {
        "source_group": "oews",
        "file_name": "oe_industry.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.industry",
        "description": "OEWS industry lookup table",
        "required": True,
    },
    {
        "source_group": "oews",
        "file_name": "oe_datatype.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.datatype",
        "description": "OEWS datatype lookup table for wage and employment measures",
        "required": True,
    },
    {
        "source_group": "oews",
        "file_name": "oe_footnote.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.footnote",
        "description": "OEWS footnote lookup table",
        "required": False,
    },
    {
        "source_group": "oews",
        "file_name": "oe_documentation.txt",
        "url": "https://download.bls.gov/pub/time.series/oe/oe.txt",
        "description": "OEWS bulk data documentation",
        "required": True,
    },

    # -----------------------------
    # BLS Employment Projections
    # -----------------------------
    {
        "source_group": "employment_projections",
        "file_name": "employment_projections_occupation_tables_2024_2034.xlsx",
        "url": "https://www.bls.gov/emp/ind-occ-matrix/occupation.xlsx",
        "description": "BLS occupational projections tables with employment, growth, openings, education, training, and wages",
        "required": True,
    },
    {
        "source_group": "employment_projections",
        "file_name": "national_employment_matrix_2024_2034.xlsx",
        "url": "https://www.bls.gov/emp/ind-occ-matrix/matrix.xlsx",
        "description": "BLS National Employment Matrix industry-occupation data",
        "required": False,
    },
    {
        "source_group": "crosswalks",
        "file_name": "onet_soc_to_nem_crosswalk.xlsx",
        "url": "https://www.bls.gov/emp/classifications-crosswalks/nem-onet-to-soc-crosswalk.xlsx",
        "description": "O*NET-SOC to BLS National Employment Matrix and Occupational Outlook Handbook crosswalk",
        "required": True,
    },
    {
        "source_group": "crosswalks",
        "file_name": "nem_occupational_coverage.xlsx",
        "url": "https://www.bls.gov/emp/classifications-crosswalks/nem-occupational-coverage.xlsx",
        "description": "BLS occupational employment directory and occupational coverage reference",
        "required": True,
    },
]


def build_request(url: str) -> Request:
    return Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; WorkforceAnalyticsLakehouse/1.0)",
            "Accept": "application/octet-stream,text/plain,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
            "Connection": "close",
        },
    )


def download_file_once(url: str, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    request = build_request(url)

    with urlopen(request, timeout=240) as response:
        status_code = getattr(response, "status", None)

        if status_code and status_code >= 400:
            raise RuntimeError(f"HTTP {status_code}")

        bytes_written = 0

        with open(output_path, "wb") as file:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break

                file.write(chunk)
                bytes_written += len(chunk)

    if bytes_written == 0:
        raise RuntimeError("Downloaded file is empty")

    return bytes_written


def download_file_with_retries(url: str, output_path: Path, max_retries: int = MAX_RETRIES) -> dict:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempt {attempt}/{max_retries}: {url}")

            bytes_written = download_file_once(url, output_path)

            return {
                "status": "success",
                "bytes_written": bytes_written,
                "error_message": None,
            }

        except HTTPError as error:
            last_error = f"HTTPError {error.code}: {error.reason}"
            print(f"Download failed: {last_error}")

        except URLError as error:
            last_error = f"URLError: {error.reason}"
            print(f"Download failed: {last_error}")

        except Exception as error:
            last_error = str(error)
            print(f"Download failed: {last_error}")

        if attempt < max_retries:
            sleep_time = RETRY_SLEEP_SECONDS * attempt
            print(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

    return {
        "status": "failed",
        "bytes_written": 0,
        "error_message": last_error,
    }


def save_manifest(records, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(records, file, indent=4)


def main():
    fetched_at = datetime.now(timezone.utc).isoformat()
    manifest_records = []

    for file_info in BLS_CORE_FILES:
        source_group = file_info["source_group"]
        file_name = file_info["file_name"]
        url = file_info["url"]
        description = file_info["description"]
        required = file_info["required"]

        output_path = BRONZE_BLS_PATH / source_group / file_name

        print(f"\nDownloading: {description}")
        print(f"Target path: {output_path}")

        result = download_file_with_retries(url, output_path)

        manifest_record = {
            "source": "BLS",
            "source_group": source_group,
            "description": description,
            "source_url": url,
            "file_name": file_name,
            "raw_file_path": str(output_path),
            "required": required,
            "download_status": result["status"],
            "bytes_written": result["bytes_written"],
            "error_message": result["error_message"],
            "fetched_at_utc": fetched_at,
        }

        manifest_records.append(manifest_record)

        if result["status"] == "success":
            print(f"Saved: {output_path}")
        else:
            print(f"Failed: {description}")

            if required:
                print("This is a required file. The failure is recorded in the manifest.")

    manifest_path = BRONZE_BLS_PATH / "_manifest.json"
    save_manifest(manifest_records, manifest_path)

    failed_required_files = [
        record for record in manifest_records
        if record["required"] and record["download_status"] == "failed"
    ]

    print("\nBLS Bronze ingestion complete.")
    print(f"Manifest saved to: {manifest_path}")

    if failed_required_files:
        print("\nWARNING: Some required files failed to download:")
        for record in failed_required_files:
            print(f"- {record['file_name']}: {record['error_message']}")

        raise RuntimeError("One or more required BLS files failed to download. Check the manifest.")

    print("All required BLS occupation-level files were downloaded successfully.")


if __name__ == "__main__":
    main()