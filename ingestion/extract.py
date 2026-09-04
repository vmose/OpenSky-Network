import os
import requests
import pandas as pd
from datetime import datetime

# 1. API Configuration
# Anonymous access is heavily rate-limited. Register a free account at opensky-network.org
OPENSKY_USER = os.getenv("OPENSKY_USER", "")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "")

API_URL = "https://opensky-network.org/api/states/all"

# Optional: Define a bounding box to filter flights by location (e.g., North America/US East Coast)
# Leave as None to fetch all active global flights (Warning: returns a massive payload)
BOUNDING_BOX = {
    "lamin": 24.396308,  # Minimum latitude
    "lomin": -125.00000, # Minimum longitude
    "lamax": 49.384358,  # Maximum latitude
    "lomax": -66.93457   # Maximum longitude
}

def extract_live_flights():
    """Fetches real-time state vectors from OpenSky Network API."""
    params = {}
    auth = None

    # Apply bounding box constraints if defined
    if BOUNDING_BOX:
        params.update(BOUNDING_BOX)

    # Apply basic authentication if credentials are provided
    if OPENSKY_USER and OPENSKY_PASSWORD:
        auth = (OPENSKY_USER, OPENSKY_PASSWORD)
        print(f"[{datetime.now()}] Fetching data using authenticated account: {OPENSKY_USER}...")
    else:
        print(f"[{datetime.now()}] Fetching data anonymously (Subject to strict rate limits)...")

    try:
        response = requests.get(API_URL, params=params, auth=auth, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from OpenSky API: {e}")
        return None

    # 2. Parse JSON response
    states = data.get("states", [])
    if not states:
        print("No flight vectors found for the given criteria.")
        return None

    # OpenSky returns data as a list of lists. We must map them back to their defined keys.
    columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]

    df = pd.DataFrame(states, columns=columns)
    
    # Clean up formatting: strip empty spaces from callsigns
    df["callsign"] = df["callsign"].str.strip()
    
    # Convert epoch timestamps to human-readable times
    df["time_position"] = pd.to_datetime(df["time_position"], unit="s", errors="coerce")
    df["last_contact"] = pd.to_datetime(df["last_contact"], unit="s", errors="coerce")

    print(f"Successfully extracted {len(df)} active flights.")
    return df

if __name__ == "__main__":
    # Run extraction
    flight_df = extract_live_flights()

    if flight_df is not None:
        # Display sample data
        print("\n--- First 5 Active Flights ---")
        print(flight_df[["icao24", "callsign", "origin_country", "latitude", "longitude", "velocity"]].head())

        # Save to CSV
        output_file = "live_flights.csv"
        flight_df.to_csv(output_file, index=False)
        print(f"\nData successfully saved to {output_file}")
