import os
import requests
import json
import gzip
import shutil
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    # TODO Implement download

    #Before downloading, it clears the existing directory
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir) 
    os.makedirs(download_dir, exist_ok=True)

    num_downloaded = 0

    # Only download files that exist (increment by 5)
    for i in range(0, 5000, 5):  
        if num_downloaded >= file_limit:
            break  

        file_name = f"{str(i).zfill(6)}Z.json.gz"
        file_url = base_url + file_name
        file_path = Path(download_dir) / file_name

        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            num_downloaded += 1  
        else:
            print(f"File not found: {file_url}, skipping...")

    return f"Downloaded {num_downloaded} files to {download_dir}"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO

    raw_dir = Path(settings.raw_dir) / "day=20231101"
    prepared_dir = Path(settings.prepared_dir) / "day=20231101"

    #Before processing, it clears the prepared directory
    if prepared_dir.exists():
        shutil.rmtree(prepared_dir)
    prepared_dir.mkdir(parents=True, exist_ok=True)

    processed_files = 0
    errors = 0

    for json_file in raw_dir.glob("*.json.gz"):  
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            print(f"✅ Read {json_file.name} as plain JSON.")

            if "aircraft" not in raw_data:
                print(f"ERROR: 'aircraft' key missing in {json_file.name}")
                continue  # Skip this file

            processed_data = []
            for entry in raw_data["aircraft"]:
                if all(key in entry for key in ["hex", "lat", "lon", "alt_baro"]):
                    processed_data.append({
                        "icao": entry["hex"],
                        "registration": entry.get("r", "Unknown"),  # Extract registration
                        "type": entry.get("t", "Unknown"),  # Extract aircraft type
                        "timestamp": entry.get("seen", 0),
                        "lat": entry["lat"],
                        "lon": entry["lon"],
                        "altitude_baro": entry["alt_baro"],
                        "ground_speed": entry.get("gs", 0),
                        "emergency": entry.get("emergency", False)  
                    })

            if not processed_data:
                print(f"WARNING: No aircraft data extracted from {json_file.name}")

            output_file = Path(prepared_dir) / f"{json_file.stem.replace('.json', '')}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, indent=4)

            print(f"Saved processed file: {output_file}")

            print(f"Processed {len(processed_data)} aircraft entries from {json_file.name}")
            processed_files += 1

        except Exception as e:
            errors += 1
            print(f"Error processing {json_file.name}: {str(e)}")

    return (
        f"Preparation complete. Processed {processed_files} files. "
        f"Encountered {errors} errors."
    )


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO
    prepared_dir = Path(settings.prepared_dir) / "day=20231101"
    aircraft_data = {}
    for json_file in prepared_dir.glob("*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                for entry in data:
                    icao = entry.get("icao")
                    registration = entry.get("registration", "Unknown")
                    aircraft_type = entry.get("type", "Unknown")
                    if icao and icao not in aircraft_data:
                        aircraft_data[icao] = {
                            "icao": icao,
                            "registration": registration,
                            "type": aircraft_type,
                        }
        except Exception as e:
            print(f"Error reading {json_file.name}: {e}")

    sorted_aircraft = sorted(aircraft_data.values(), key=lambda x: x["icao"])
    start_idx = page * num_results
    return sorted_aircraft[start_idx: start_idx + num_results]


    # return [{"icao": "0d8300", "registration": "YV3382", "type": "LJ31"}]









@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.
    return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}
