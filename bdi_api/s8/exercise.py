from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import create_engine, text

from bdi_api.s8.models import Aircraft, AircraftCO2Data
from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()

# Create database connection
DATABASE_URL = f"postgresql://{db_credentials.username}:{db_credentials.password}@{db_credentials.host}:{db_credentials.port}/{db_credentials.database}"
engine = create_engine(DATABASE_URL)

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

@s8.get("/aircraft/", response_model=list[Aircraft])
def list_aircraft(num_results: int = 100, page: int = 0) -> list[Aircraft]:
    """List all the available aircraft with their details from the database.

    Args:
        num_results: Number of results to return per page
        page: Page number (0-based) for pagination

    Returns:
        List of Aircraft objects with registration, type, owner, manufacturer and model information
    """
    offset = page * num_results

    query = text("""
        WITH unique_aircraft AS (
            SELECT DISTINCT icao
            FROM readsb_aircraft_data
        )
        SELECT
            ua.icao,
            adb.registration,
            adb.icao_aircraft_type as type,
            adb.owner as owner,
            adb.manufacturer,
            adb.model
        FROM unique_aircraft ua
        LEFT JOIN aircraft_db adb ON ua.icao = adb.icao
        ORDER BY ua.icao ASC
        LIMIT :limit OFFSET :offset
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(
                query,
                {"limit": num_results, "offset": offset}
            )

            return [
                Aircraft(
                    icao=row.icao,
                    registration=row.registration,
                    type=row.type,
                    owner=row.owner,
                    manufacturer=row.manufacturer,
                    model=row.model
                )
                for row in result
            ]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )

@s8.get("/aircraft/{icao}/co2", response_model=AircraftCO2Data)
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Data:
    """Calculate CO2 emissions for a specific aircraft on a given day.

    Args:
        icao: Aircraft ICAO identifier
        day: Date in YYYY-MM-DD format

    Returns:
        AircraftCO2Data object with hours flown and CO2 emissions (if available)
    """
    try:
        # Validate and parse the date
        try:
            day_date = datetime.strptime(day, "%Y-%m-%d")
            next_day = datetime.strptime(f"{day} 23:59:59", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        # Query to get aircraft type and flight hours
        query = text("""
            WITH flight_data AS (
                SELECT
                    rad.icao,
                    adb.icao_aircraft_type as type,
                    COUNT(*) * 5.0 / 3600.0 as hours  -- Each row is 5 seconds
                FROM readsb_aircraft_data rad
                LEFT JOIN aircraft_db adb ON rad.icao = adb.icao
                WHERE rad.icao = :icao
                AND rad.timestamp >= :start_day
                AND rad.timestamp < :end_day
                GROUP BY rad.icao, adb.icao_aircraft_type
            )
            SELECT
                fd.icao,
                fd.type,
                fd.hours as hours_flown,
                afc.fuel_consumption_rate as galph
            FROM flight_data fd
            LEFT JOIN aircraft_fuel_consumption afc ON fd.type = afc.aircraft_type
        """)

        with engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    "icao": icao,
                    "start_day": day_date,
                    "end_day": next_day
                }
            ).first()

            if not result:
                return AircraftCO2Data(icao=icao, hours_flown=0, co2=None)

            hours_flown = float(result.hours_flown)

            # Calculate CO2 if we have fuel consumption data
            co2 = None
            if result.galph is not None:
                fuel_used_gal = result.galph * hours_flown
                fuel_used_kg = fuel_used_gal * 3.04
                co2 = (fuel_used_kg * 3.15) / 907.185

            return AircraftCO2Data(
                icao=icao,
                hours_flown=hours_flown,
                co2=co2
            )

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating CO2: {str(e)}"
        )
