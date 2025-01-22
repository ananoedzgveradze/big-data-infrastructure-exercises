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
    """Downloads the file_limit files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the pyproject.toml file
    so it can be installed using poetry update.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    print(f"Saving files in: {download_dir}")  # Debugging print statement

    # Ensure the directory exists
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)  # Delete old files
    os.makedirs(download_dir, exist_ok=True)

    # Fetch file list from ADSBExchange
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.RequestException as e:
        return f"Error accessing {BASE_URL}: {e}"

    soup = BeautifulSoup(response.text, "html.parser")
    file_links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")]

    if not file_links:
        return "No files found"
    
    files_to_download = file_links[:file_limit]
    print(f"Downloading {len(files_to_download)} files...")


    for file_name in files_to_download:
        file_url = BASE_URL + file_name  # Full URL of the file
        file_path = os.path.join(download_dir, file_name)

        try:
            file_response = requests.get(file_url, stream=True)
            file_response.raise_for_status()
            
            with open(file_path, "wb") as file:
                file.write(file_response.content)

            print(f"Downloaded {file_name} -> {file_path}")

        except requests.exceptions.RequestException as e:
            print(f"Skipping {file_name}, Error: {e}")

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
    Just make sure to configure it in the pyproject.toml file
    so it can be installed using poetry update.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO
    
    #define directories
    raw_data_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepare_data_dir = os.path.join(settings.raw_dir, "prepared")

    if os.path.exists(prepare_data_dir):
        shutil.rmtree(prepare_data_dir)
    os.makedirs(prepare_data_dir, exist_ok=True)

    processed_data = []
    invalid_entries = 0  # Counter for invalid entries

    for file_name in os.listdir(raw_data_dir):
        file_path = os.path.join(raw_data_dir, file_name)

        # Check if the file is gzipped
        try:
            with open(file_path, "rb") as f:
                first_bytes = f.read(2)

            is_gzipped = first_bytes == b'\x1f\x8b'  # Gzip magic number

            if is_gzipped:
                open_func = gzip.open
                mode = "rt"
            else:
                open_func = open
                mode = "r"

            print(f"Processing {file_name}... (GZ: {is_gzipped})")

            with open_func(file_path, mode, encoding="utf-8") as f:
                data = json.load(f)

            if "aircraft" not in data:
                print(f"Skipping {file_name}: No 'aircraft' key.")
                continue

            for entry in data["aircraft"]:
                # Validate record
                if not entry.get("hex") or entry.get("hex") in ["000000", "000001"]:
                    invalid_entries += 1
                    continue  # Skip invalid ICAO

                if not entry.get("lat") or not entry.get("lon"):
                    invalid_entries += 1
                    continue  # Skip records with missing coordinates

                if not entry.get("t"):
                    invalid_entries += 1
                    continue  # Skip records with unknown aircraft type

                processed_entry = {
                    "icao": entry.get("hex", "").strip(),
                    "flight": entry.get("flight", "").strip(),
                    "lat": entry.get("lat"),
                    "lon": entry.get("lon"),
                    "altitude": entry.get("alt_baro"),
                    "speed": entry.get("gs"),
                    "timestamp": data.get("now"),
                    "aircraft_type": entry.get("t"),
                    "registration": entry.get("r", "Unknown"),
                    "positions": [{"timestamp": data.get("now"), "lat": entry.get("lat"), "lon": entry.get("lon")}],
                }
                processed_data.append(processed_entry)

        except json.JSONDecodeError as e:
            print(f"JSON error in {file_name}: {e}")
        except Exception as e:
            print(f"Error processing {file_name}: {e}")

    output_file = os.path.join(prepare_data_dir, "processed_data.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(processed_data, f, indent=4)

    print(f"Processed {len(processed_data)} valid aircraft records. ❌ Removed {invalid_entries} invalid records.")
    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO
    try:
        processed_data_file = os.path.join(settings.raw_dir, "prepared", "processed_data.json")

        if not os.path.exists(processed_data_file):
            raise FileNotFoundError(f"{processed_data_file} not found.")

        with open(processed_data_file, "r", encoding="utf-8") as f:
            processed_data = json.load(f)

        print(f"Loaded {len(processed_data)} records from {processed_data_file}")

        # **Filter out invalid data**
        valid_aircraft = [
            aircraft for aircraft in processed_data
            if aircraft["icao"] not in ["000000", "000001", "000860", "00b2f0", "Unknown", ""]
            and aircraft["lat"] is not None
            and aircraft["lon"] is not None
            and aircraft["altitude"] not in [None, "ground"]  # Remove ground-level aircraft
            and aircraft["flight"] not in [None, ""]  # Remove aircraft with no flight number
            and aircraft["aircraft_type"] not in ["Unknown", ""]  # Remove unknown aircraft types
        ]
        print(f"Filtered out {len(processed_data) - len(valid_aircraft)} invalid aircraft")

        # Sort by ICAO
        sorted_data = sorted(valid_aircraft, key=lambda x: x.get("icao", "ZZZZZZ"))
        print(f"First 5 records after sorting: {sorted_data[:5]}")

        # Pagination
        paginated_data = sorted_data[page * num_results: (page + 1) * num_results]
        print(f"Sending {len(paginated_data)} aircraft for page {page}")
        print(f"First 5 of paginated: {paginated_data[:5]}")

        return paginated_data

    except Exception as e:
        print(f"Error: {e}")
        return {"error": str(e)}
   
    # return [{"icao": "0d8300", "registration": "YV3382", "type": "LJ31"}]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.
    try:
        # Load the processed data
        processed_data_file = os.path.join(settings.raw_dir, "prepared", "processed_data.json")
        if not os.path.exists(processed_data_file):
            raise HTTPException(status_code=404, detail="Processed data not found.")

        with open(processed_data_file, "r", encoding="utf-8") as f:
            processed_data = json.load(f)

        # Find the aircraft data for the specified ICAO
        aircraft_data = [entry for entry in processed_data if entry["icao"] == icao]
        if not aircraft_data:
            print(f"No data found for ICAO {icao}")
            raise HTTPException(status_code=404, detail="Aircraft not found")
        # Get positions for the aircraft
        positions = []
        for entry in aircraft_data:
            positions.extend(entry.get("positions", []))

        # Paginate the positions list
        start = page * num_results
        end = start + num_results
        paginated_positions = positions[start:end]
        
        return paginated_positions

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
   
    try:
        processed_data_file = os.path.join(settings.raw_dir, "prepared", "processed_data.json")
        if not os.path.exists(processed_data_file):
            raise HTTPException(status_code=404, detail="Processed data not found.")

        with open(processed_data_file, "r", encoding="utf-8") as f:
            processed_data = json.load(f)

        # Find the aircraft data for the specified ICAO
        aircraft_data = [entry for entry in processed_data if entry["icao"] == icao]
        if not aircraft_data:
            print(f"No data found for ICAO {icao}")  # Debugging statement
            raise HTTPException(status_code=404, detail=f"Aircraft with ICAO {icao} not found.")


        # Gather statistics
        max_altitude_baro = max(entry["altitude"] for entry in aircraft_data if entry["altitude"] is not None)
        max_ground_speed = max(entry["speed"] for entry in aircraft_data if entry["speed"] is not None)
        had_emergency = any(entry.get("emergency", False) for entry in aircraft_data)

        return {
            "max_altitude_baro": max_altitude_baro,
            "max_ground_speed": max_ground_speed,
            "had_emergency": had_emergency,
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
    return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}
