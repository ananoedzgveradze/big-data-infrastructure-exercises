from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Aircraft(BaseModel):
    icao: str
    registration: Optional[str] = None
    type: Optional[str] = None
    owner: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None

    class Config:
        from_attributes = True

class AircraftData(BaseModel):
    timestamp: datetime
    icao: str
    flight: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    altitude: Optional[int] = None
    speed: Optional[float] = None
    track: Optional[float] = None
    vertical_rate: Optional[int] = None
    squawk: Optional[str] = None
    emergency: Optional[str] = None
    category: Optional[str] = None
    nav_qnh: Optional[float] = None
    nav_altitude_mcp: Optional[float] = None
    nav_heading: Optional[float] = None
    version: Optional[str] = None

    class Config:
        from_attributes = True

class FuelConsumption(BaseModel):
    aircraft_type: str
    name: Optional[str] = None
    fuel_consumption_rate: Optional[float] = None
    category: Optional[str] = None
    source: Optional[str] = None

    class Config:
        from_attributes = True

class AircraftCO2Data(BaseModel):
    icao: str
    hours_flown: float
    co2: Optional[float] = None

    class Config:
        from_attributes = True
