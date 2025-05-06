# System Architecture

```mermaid
graph TD
    %% External Data Sources
    ExternalData[("External Data Sources")]
    ADSB["ADSB Exchange"]
    AircraftDB["Aircraft Database"]
    FuelData["Fuel Consumption Data"]
    
    %% AWS Services
    S3[("AWS S3 Bucket")]
    subgraph S3_Content["S3 Storage"]
        Raw["/raw"]
        Processed["/processed"]
    end

    %% Airflow Server
    subgraph AirflowServer["Airflow Server"]
        DAGs["DAGs"]
        Scheduler["Scheduler"]
        Workers["Workers"]
    end

    %% Database
    PostgreSQL[("PostgreSQL Database")]
    subgraph DB_Tables["Database Tables"]
        Aircraft["Aircraft Data"]
        FlightData["Flight Data"]
        FuelRates["Fuel Consumption Rates"]
    end

    %% FastAPI Application
    subgraph API["FastAPI Application"]
        Endpoints["REST Endpoints"]
        Models["Pydantic Models"]
        ErrorHandling["Error Handling"]
    end

    %% Connections and Data Flow
    ExternalData --> |Data Sources| ADSB
    ExternalData --> |Data Sources| AircraftDB
    ExternalData --> |Data Sources| FuelData

    ADSB --> |Extract| AirflowServer
    AircraftDB --> |Extract| AirflowServer
    FuelData --> |Extract| AirflowServer

    AirflowServer --> |Store Raw Data| Raw
    AirflowServer --> |Store Processed| Processed
    
    Raw --> |Transform| Processed
    Processed --> |Load| PostgreSQL

    PostgreSQL --> |Query| API
    API --> |Serve Data| Users((Users))

    %% Styling
    classDef primary fill:#2374ab,stroke:#2374ab,stroke-width:2px,color:#fff
    classDef secondary fill:#ff7e67,stroke:#ff7e67,stroke-width:2px,color:#fff
    classDef storage fill:#78a042,stroke:#78a042,stroke-width:2px,color:#fff
    classDef compute fill:#845ec2,stroke:#845ec2,stroke-width:2px,color:#fff

    class ExternalData,PostgreSQL,S3 storage
    class AirflowServer compute
    class API primary
    class Users secondary
```

## Component Details

### Data Sources
- **ADSB Exchange**: Real-time and historical flight data
- **Aircraft Database**: Aircraft registration and specifications
- **Fuel Consumption Data**: Aircraft type fuel consumption rates

### Airflow Server Components
- **DAGs**: 
  - `readsb_hist_dag.py`
  - `aircraft_db_dag.py`
  - `fuel_consumption_dag.py`
- **Scheduler**: Manages DAG execution
- **Workers**: Execute tasks

### Storage
- **S3 Bucket**:
  - Raw data storage
  - Processed data storage
- **PostgreSQL**:
  - Structured data storage
  - Optimized for querying

### API Application
- **Endpoints**:
  - `/api/s8/aircraft/`
  - `/api/s8/aircraft/{icao}/co2`
- **Features**:
  - Data validation
  - Error handling
  - Response formatting 