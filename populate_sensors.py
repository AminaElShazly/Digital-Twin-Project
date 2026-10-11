"""One-off Lambda: seed the Sensors metadata table (23 items).

Run once (e.g. via a Lambda test event) after creating the `Sensors` table.
Keys "14"-"21" are the unnamed channels exactly as the greenhouse API returns them.
"""
import os

import boto3

SENSORS_TABLE = os.environ.get("SENSORS_TABLE", "Sensors")

SENSORS = [
    {"sensor_key": "indoor_temperature", "display_name": "Indoor Temperature", "unit": "°C", "category": "indoor"},
    {"sensor_key": "indoor_co2", "display_name": "Indoor CO2", "unit": "ppm", "category": "indoor"},
    {"sensor_key": "indoor_vpd", "display_name": "Indoor VPD", "unit": "g/m³", "category": "indoor"},
    {"sensor_key": "outdoor_air_temp", "display_name": "Outdoor Temperature", "unit": "°C", "category": "outdoor"},
    {"sensor_key": "outdoor_rh", "display_name": "Outdoor Humidity", "unit": "%", "category": "outdoor"},
    {"sensor_key": "outdoor_wind_speed", "display_name": "Wind Speed", "unit": "m/s", "category": "outdoor"},
    {"sensor_key": "outdoor_global_radiation", "display_name": "Global Radiation", "unit": "W/m²", "category": "outdoor"},
    {"sensor_key": "energy_screen_closure", "display_name": "Energy Screen", "unit": "%", "category": "actuator"},
    {"sensor_key": "blackout_screen_closure", "display_name": "Blackout Screen", "unit": "%", "category": "actuator"},
    {"sensor_key": "lee_vent_aperture", "display_name": "Lee Vent", "unit": "%", "category": "actuator"},
    {"sensor_key": "wind_vent_aperture", "display_name": "Wind Vent", "unit": "%", "category": "actuator"},
    {"sensor_key": "pipe_rail_inlet_temp", "display_name": "Pipe Rail Temp", "unit": "°C", "category": "system"},
    {"sensor_key": "grow_pipes_inlet_temp", "display_name": "Grow Pipes Temp", "unit": "°C", "category": "system"},
    {"sensor_key": "lights_on", "display_name": "Lights", "unit": "boolean", "category": "actuator"},
    {"sensor_key": "co2_injection_status", "display_name": "CO2 Injection", "unit": "boolean", "category": "actuator"},
] + [
    {"sensor_key": str(n), "display_name": f"Sensor {n}", "unit": "", "category": "system"}
    for n in range(14, 22)
]


def lambda_handler(event, context):
    table = boto3.resource("dynamodb").Table(SENSORS_TABLE)
    for sensor in SENSORS:
        table.put_item(Item=sensor)
        print(f"Added: {sensor['sensor_key']}")
    return {"statusCode": 200, "body": f"Added {len(SENSORS)} sensors"}
