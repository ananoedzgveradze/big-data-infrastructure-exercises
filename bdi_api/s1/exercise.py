import os
import json #for handling JSON data
import gzip #for extracting .gz compressed files
import requests #For  HTTP requests to download data
import shutil # For deleting old files before downloading new ones
from bs4 import BeautifulSoup  # For deleting old files before downloading new ones
from typing import Annotated
from typing import List, Dict
from fastapi import APIRouter, status
from fastapi import HTTPException
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

# Base URL for downloading aircraft tracking data
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"


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
    print(f"📂 Saving files in: {download_dir}")  # Debugging print statement

    # ✅ Ensure the directory exists
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)  # Delete old files
    os.makedirs(download_dir, exist_ok=True)

    # ✅ Fetch file list from ADSBExchange
    response = requests.get(BASE_URL)
    if response.status_code != 200:
        print(f"❌ Failed to access {BASE_URL}, HTTP {response.status_code}")
        return f"Failed to access {BASE_URL}, HTTP {response.status_code}"
    
    soup = BeautifulSoup(response.text, "html.parser")
    file_links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")]


    if len(file_links) == 0:
        print("❌ No files found for download.")
        return "No files found"

    # ✅ Limit downloads
    files_to_download = file_links[:file_limit]
    print(f"📥 Downloading {len(files_to_download)} files...")

    # ✅ Download each file
    for file_name in files_to_download:
        file_url = BASE_URL + file_name

    # Remove .gz extension since it's plain JSON
        if file_name.endswith(".json.gz"):
            file_name = file_name[:-3]  # Remove `.gz`

    file_path = os.path.join(download_dir, file_name)
    print(f"⬇ Downloading {file_name}")

    try:
        file_response = requests.get(file_url, stream=True)
        if file_response.status_code == 200:
            with open(file_path, "wb") as file:
                file.write(file_response.content)
            print(f"✅ Downloaded {file_name}")
        else:
            print(f"⚠️ Skipping {file_name}, HTTP {file_response.status_code}")
    except Exception as e:
            print(f"❌ Error downloading {file_name}: {e}")

    return "OK"


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
    
    #define directories
    raw_data_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepare_data_dir = os.path.join(settings.raw_dir, "prepared")


    # Ensure the prepared data directory is clean
    if os.path.exists(prepare_data_dir):
        shutil.rmtree(prepare_data_dir)
    os.makedirs(prepare_data_dir, exist_ok=True)

    # List all downloaded JSON files (both .json and .json.gz)
    raw_files = [f for f in os.listdir(raw_data_dir) if f.endswith(".json") or f.endswith(".json.gz")]

    processed_data = []

    # Read and process each file
    for file_name in raw_files:
        file_path = os.path.join(raw_data_dir, file_name)

        try:
            # Open files correctly based on extension
            if file_name.endswith(".json.gz"):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

            # Check if the JSON has an "aircraft" key
            if "aircraft" in data:
                aircraft_list = data["aircraft"]
            else:
                print(f"⚠️ Skipping {file_name}: No 'aircraft' key found.")
                continue  # Skip this file if no aircraft data is found

            for entry in aircraft_list:
                processed_entry = {
                    "icao": entry.get("hex", "Unknown"),
                    "flight": entry.get("flight", "").strip(),
                    "lat": entry.get("lat"),
                    "lon": entry.get("lon"),
                    "altitude": entry.get("alt_baro"),
                    "speed": entry.get("gs"),
                    "timestamp": data.get("now", None),  # Use "now" from JSON as timestamp
                    "aircraft_type": entry.get("t", "Unknown"),
                    "registration": entry.get("r", "Unknown"),
                }
                processed_data.append(processed_entry)

        except Exception as e:
            print(f"❌ Error processing file {file_name}: {e}")

    # Save processed data to JSON
    output_file = os.path.join(prepare_data_dir, "processed_data.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(processed_data, f, indent=4)

    print(f"✅ Processed data saved to {output_file}")

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO

    try:
        # Define the path to the processed data file
        processed_data_file = os.path.join(settings.raw_dir, "prepared", "processed_data.json")

        # Check if the file exists
        if not os.path.exists(processed_data_file):
            raise FileNotFoundError(f"{processed_data_file} not found.")

        # Read the processed data from the file
        with open(processed_data_file, "r", encoding="utf-8") as f:
            processed_data = json.load(f)

        # Sort data by ICAO code (ascending order)
        sorted_data = sorted(processed_data, key=lambda x: x["icao"])

        # Apply pagination
        start = page * num_results
        end = start + num_results
        paginated_data = sorted_data[start:end]

        # Return the paginated list of aircraft
        return paginated_data
    
    except FileNotFoundError as e:
        return {"error": str(e)}, status.HTTP_404_NOT_FOUND
    except Exception as e:
        return {"error": f"An error occurred: {e}"}, status.HTTP_500_INTERNAL_SERVER_ERROR
    
    return [{"icao": "0d8300", "registration": "YV3382", "type": "LJ31"}]


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
