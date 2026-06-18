import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

BRONZE_BLS_PATH = Path("/Volumes/workforce_analytics/bronze/raw_files/bls/api")


BLS_SERIES_GROUPS = {
    "jolts_national": [
        {
            "series_id": "JTS000000000000000JOL",
            "file_name": "job_openings_level_latest.json",
            "description": "National job openings level",
        },
        {
            "series_id": "JTS000000000000000JOR",
            "file_name": "job_openings_rate_latest.json",
            "description": "National job openings rate",
        },
        {
            "series_id": "JTS000000000000000HIL",
            "file_name": "hires_level_latest.json",
            "description": "National hires level",
        },
        {
            "series_id": "JTS000000000000000HIR",
            "file_name": "hires_rate_latest.json",
            "description": "National hires rate",
        },
        {
            "series_id": "JTS000000000000000QUL",
            "file_name": "quits_level_latest.json",
            "description": "National quits level",
        },
        {
            "series_id": "JTS000000000000000QUR",
            "file_name": "quits_rate_latest.json",
            "description": "National quits rate",
        },
        {
            "series_id": "JTS000000000000000LDL",
            "file_name": "layoffs_discharges_level_latest.json",
            "description": "National layoffs and discharges level",
        },
        {
            "series_id": "JTS000000000000000LDR",
            "file_name": "layoffs_discharges_rate_latest.json",
            "description": "National layoffs and discharges rate",
        },
        {
            "series_id": "JTS000000000000000TSL",
            "file_name": "total_separations_level_latest.json",
            "description": "National total separations level",
        },
        {
            "series_id": "JTS000000000000000TSR",
            "file_name": "total_separations_rate_latest.json",
            "description": "National total separations rate",
        },
    ],

    "cps_context": [
        {
            "series_id": "LNS14000000",
            "file_name": "unemployment_rate_latest.json",
            "description": "National unemployment rate",
        },
        {
            "series_id": "LNS11300000",
            "file_name": "labor_force_participation_rate_latest.json",
            "description": "National labor force participation rate",
        },
        {
            "series_id": "LNS12300000",
            "file_name": "employment_population_ratio_latest.json",
            "description": "National employment-population ratio",
        },
        {
            "series_id": "LNS11000000",
            "file_name": "civilian_labor_force_level_latest.json",
            "description": "National civilian labor force level",
        },
        {
            "series_id": "LNS12000000",
            "file_name": "civilian_employment_level_latest.json",
            "description": "National civilian employment level",
        },
        {
            "series_id": "LNS13000000",
            "file_name": "civilian_unemployment_level_latest.json",
            "description": "National civilian unemployment level",
        },
    ],
}


def build_bls_latest_url(api_url, series_id, api_key=None):
    base_url = api_url.rstrip("/")
    query_params = {"latest": "true"}

    if api_key:
        query_params["registrationkey"] = api_key

    query = urlencode(query_params)
    return f"{base_url}/{series_id}?{query}"


def fetch_raw_json(url):
    request = Request(url, headers={"Accept": "application/json"})

    try:
        with urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        raise RuntimeError(f"BLS request failed with HTTP {error.code}") from error
    except URLError as error:
        raise RuntimeError(f"BLS request failed: {error.reason}") from error


def save_raw_json(raw_json, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        file.write(raw_json)


def save_manifest(manifest_records, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(manifest_records, file, indent=4)


def main():
    api_key = os.getenv("BLS_API_KEY")
    api_url = os.getenv("BLS_API_URL", DEFAULT_BLS_API_URL)

    manifest_records = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for group_name, series_list in BLS_SERIES_GROUPS.items():
        group_path = BRONZE_BLS_PATH / group_name
        group_path.mkdir(parents=True, exist_ok=True)

        for series_info in series_list:
            series_id = series_info["series_id"]
            file_name = series_info["file_name"]
            description = series_info["description"]

            request_url = build_bls_latest_url(api_url, series_id, api_key)
            raw_json = fetch_raw_json(request_url)

            raw_json_path = group_path / file_name
            save_raw_json(raw_json, raw_json_path)

            manifest_records.append({
                "source": "BLS API v2",
                "group": group_name,
                "series_id": series_id,
                "description": description,
                "file_name": file_name,
                "raw_file_path": str(raw_json_path),
                "fetched_at_utc": fetched_at,
            })

            print(f"Saved {description}: {raw_json_path}")

            time.sleep(0.5)

    manifest_path = BRONZE_BLS_PATH / "_manifest.json"
    save_manifest(manifest_records, manifest_path)

    print("BLS raw JSON files saved to Bronze volume.")
    print(f"Manifest saved to: {manifest_path}")


if __name__ == "__main__":
    main()