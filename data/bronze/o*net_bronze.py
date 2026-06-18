import json
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ONET_TEXT_ZIP_URL = "https://www.onetcenter.org/dl_files/database/db_30_3_text.zip"

BRONZE_ONET_PATH = Path("/Volumes/workforce_analytics/bronze/raw_files/onet")
RAW_ZIP_PATH = BRONZE_ONET_PATH / "raw_zip"
EXTRACTED_TEXT_PATH = BRONZE_ONET_PATH / "text_files"
MANIFEST_PATH = BRONZE_ONET_PATH / "_manifest.json"

ZIP_FILE_NAME = "onet_database_30_3_text.zip"


def download_file(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/zip,application/octet-stream,*/*",
        },
    )

    try:
        with urlopen(request, timeout=120) as response:
            with open(output_path, "wb") as file:
                file.write(response.read())

    except HTTPError as error:
        raise RuntimeError(f"O*NET download failed with HTTP {error.code}") from error

    except URLError as error:
        raise RuntimeError(f"O*NET download failed: {error.reason}") from error


def extract_text_files(zip_path: Path, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    extracted_files = []

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.namelist():
            member_path = Path(member)

            if member.endswith("/"):
                continue

            if member_path.suffix.lower() != ".txt":
                continue

            # Keep only the file name to avoid nested zip folder issues
            target_path = output_dir / member_path.name

            with zip_ref.open(member) as source:
                with open(target_path, "wb") as target:
                    target.write(source.read())

            extracted_files.append(
                {
                    "source_file_in_zip": member,
                    "saved_file_name": member_path.name,
                    "saved_file_path": str(target_path),
                }
            )

    return extracted_files


def save_manifest(records, manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(records, file, indent=4)


def main():
    fetched_at = datetime.now(timezone.utc).isoformat()

    zip_output_path = RAW_ZIP_PATH / ZIP_FILE_NAME

    print("Downloading O*NET text database...")
    download_file(ONET_TEXT_ZIP_URL, zip_output_path)
    print(f"Saved raw O*NET ZIP to: {zip_output_path}")

    print("Extracting O*NET text files...")
    extracted_files = extract_text_files(zip_output_path, EXTRACTED_TEXT_PATH)

    manifest_records = {
        "source": "O*NET Database Text Download",
        "source_url": ONET_TEXT_ZIP_URL,
        "database_version": "30.3",
        "raw_zip_path": str(zip_output_path),
        "extracted_text_path": str(EXTRACTED_TEXT_PATH),
        "file_count": len(extracted_files),
        "fetched_at_utc": fetched_at,
        "files": extracted_files,
    }

    save_manifest(manifest_records, MANIFEST_PATH)

    print(f"Extracted {len(extracted_files)} O*NET text files.")
    print(f"Manifest saved to: {MANIFEST_PATH}")
    print("O*NET raw files saved to Bronze volume.")


if __name__ == "__main__":
    main()