# schema.py

OPENSKY_COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]


COLUMN_DTYPES = {
    "icao24": "string",
    "callsign": "string",
    "origin_country": "string",
    "time_position": "datetime64[ns, UTC]",
    "last_contact": "datetime64[ns, UTC]",
    "longitude": "float64",
    "latitude": "float64",
    "baro_altitude": "float64",
    "on_ground": "boolean",
    "velocity": "float64",
    "true_track": "float64",
    "vertical_rate": "float64",
    "sensors": "object",
    "geo_altitude": "float64",
    "squawk": "string",
    "spi": "boolean",
    "position_source": "Int64",
}


NULLABLE_COLUMNS = [
    "callsign",
    "baro_altitude",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
]


QUALITY_RULES = {
    "latitude": {
        "min": -90,
        "max": 90,
    },
    "longitude": {
        "min": -180,
        "max": 180,
    },
    "true_track": {
        "min": 0,
        "max": 360,
    },
    "velocity": {
        "min": 0,
    },
}