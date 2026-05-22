#!/usr/bin/env python3
"""
Minnesota Weather Poller
Hits the NOAA API for active weather alerts in Minnesota and prints them.
This is the "pump jack" — step 1 of the weather data pipeline.
"""

import requests
import json
from datetime import datetime

NOAA_ALERTS_URL = "https://api.weather.gov/alerts/active"
STATE = "MN"
HEADERS = {
    "User-Agent": "mn-weather-pipeline/0.1 (your@email.com)",  # NOAA requires a user agent
    "Accept": "application/geo+json"
}

SEVERITY_COLORS = {
    "Extreme":  "\033[91m",  # red
    "Severe":   "\033[93m",  # yellow
    "Moderate": "\033[94m",  # blue
    "Minor":    "\033[96m",  # cyan
    "Unknown":  "\033[0m",
}
RESET = "\033[0m"


def fetch_alerts():
    params = {"area": STATE}
    response = requests.get(NOAA_ALERTS_URL, headers=HEADERS, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def print_alert(alert):
    props = alert.get("properties", {})

    event      = props.get("event", "Unknown Event")
    severity   = props.get("severity", "Unknown")
    headline   = props.get("headline", "No headline")
    area       = props.get("areaDesc", "Unknown area")
    onset      = props.get("onset", "")
    expires    = props.get("expires", "")
    description = props.get("description", "").strip()

    color = SEVERITY_COLORS.get(severity, RESET)

    print(f"{color}{'='*60}{RESET}")
    print(f"{color}[{severity.upper()}] {event}{RESET}")
    print(f"  Headline : {headline}")
    print(f"  Area     : {area}")
    print(f"  Onset    : {onset}")
    print(f"  Expires  : {expires}")
    if description:
        # Print first 300 chars of description so it's readable
        short_desc = description[:300] + ("..." if len(description) > 300 else "")
        print(f"  Details  : {short_desc}")
    print()


def main():
    print(f"\n🌨  Minnesota Weather Poller")
    print(f"   Polling NOAA at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        data = fetch_alerts()
    except requests.RequestException as e:
        print(f"Error fetching alerts: {e}")
        return

    features = data.get("features", [])

    if not features:
        print("✅ No active weather alerts in Minnesota right now.")
        return

    print(f"⚠️  {len(features)} active alert(s) in Minnesota:\n")
    for alert in features:
        print_alert(alert)


if __name__ == "__main__":
    main()