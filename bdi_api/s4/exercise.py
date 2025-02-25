import boto3
import requests
import shutil
import json
import gzip
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s3 = boto3.client("s3")

def upload_to_s3(local_path, bucket_name, s3_key):
    """Uploads a file to S3 and logs progress."""
    try:
        print(f"Attempting to upload {local_path} to s3://{bucket_name}/{s3_key}")
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"Successfully uploaded {local_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Upload failed for {local_path}: {str(e)}") 

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    # TODO
    download_dir = Path("/tmp/aircraft_raw/")

    # Clean old files before downloading
    if download_dir.exists():
        shutil.rmtree(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    num_downloaded = 0
    for i in range(0, 5000, 5):  
        if num_downloaded >= file_limit:
            break

        file_name = f"{str(i).zfill(6)}Z.json.gz"
        file_url = base_url + file_name
        local_file_path = download_dir / file_name

        response = requests.get(file_url, stream=True)

        if response.status_code == 200:
            # Save file locally first
            with open(local_file_path, "wb") as f:
                f.write(response.content)

            # Upload to S3
            upload_to_s3(str(local_file_path), s3_bucket, f"{s3_prefix_path}{file_name}")
            num_downloaded += 1
        else:
            print(f"File not found: {file_url}, skipping...")

    return f"Downloaded and uploaded {num_downloaded} files to s3://{s3_bucket}/{s3_prefix_path}"



@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    prepared_dir = Path(settings.prepared_dir) / "day=20231101"
    
    # Clean the prepared directory
    if prepared_dir.exists():
        shutil.rmtree(prepared_dir)
    prepared_dir.mkdir(parents=True, exist_ok=True)

    # List objects in S3
    s3_objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)

    if "Contents" not in s3_objects:
        return "No files found in S3 bucket."

    processed_files = 0
    for obj in s3_objects["Contents"]:
        file_key = obj["Key"]
        file_name = file_key.split("/")[-1]
        local_gz_path = prepared_dir / file_name
        local_json_path = local_gz_path.with_suffix("")  # Remove double extension
        local_json_path = local_json_path.with_suffix(".json")


        # Download file from S3
        s3.download_file(s3_bucket, file_key, str(local_gz_path))

        # Extract and process data
        try:
            with gzip.open(local_gz_path, "rt", encoding="utf-8") as gz_file:
                raw_data = json.load(gz_file)
        except OSError:  # If not gzipped, read as plain JSON
            with open(local_gz_path, "r", encoding="utf-8") as json_file:
                raw_data = json.load(json_file)


            processed_data = [
                {
                    "icao": entry["hex"],
                    "registration": entry.get("r", "Unknown"),
                    "type": entry.get("t", "Unknown"),
                    "timestamp": entry.get("seen", 0),
                    "lat": entry["lat"],
                    "lon": entry["lon"],
                    "altitude_baro": entry.get("alt_baro", None),
                    "ground_speed": entry.get("gs", 0),
                    "emergency": entry.get("emergency", False),
                }
                for entry in raw_data.get("aircraft", [])
                if "lat" in entry and "lon" in entry
            ]

            # Save processed data locally
            with open(local_json_path, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, indent=4)

            print(f"Processed file: {local_json_path}")
            processed_files += 1

        except Exception as e:
            print(f"Error processing {local_gz_path}: {e}")

    return f"Processed {processed_files} files from S3."