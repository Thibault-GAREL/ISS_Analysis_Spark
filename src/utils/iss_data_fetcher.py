"""
ISS Data Fetcher - Utility to fetch real-time ISS position data from Open Notify API
"""
import requests
import time
import json
from typing import Optional, Dict
from datetime import datetime


class ISSDataFetcher:
    """Fetches real-time ISS position data from Open Notify API."""

    ISS_POSITION_URL = "http://api.open-notify.org/iss-now.json"
    ISS_ASTROS_URL = "http://api.open-notify.org/astros.json"

    def __init__(self, max_retries: int = 3):
        """
        Initialize the ISS data fetcher.

        Args:
            max_retries: Maximum number of retry attempts for failed requests
        """
        self.max_retries = max_retries

    def fetch_position(self) -> Optional[Dict]:
        """
        Fetch current ISS position.

        Returns:
            Dictionary containing latitude, longitude, timestamp, and fetch time
            Returns None if all retry attempts fail
        """
        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.ISS_POSITION_URL, timeout=10)
                response.raise_for_status()
                data = response.json()

                if data.get('message') == 'success':
                    position = data['iss_position']
                    return {
                        'latitude': float(position['latitude']),
                        'longitude': float(position['longitude']),
                        'timestamp': int(data['timestamp']),
                        'fetch_time': datetime.now().isoformat(),
                        'altitude_km': 408.0  # Average ISS altitude
                    }
            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    print(f"Error fetching ISS position after {self.max_retries} attempts: {e}")
                    return None
                wait_time = 2 ** attempt
                time.sleep(wait_time)

        return None

    def fetch_astronauts(self) -> Optional[Dict]:
        """
        Fetch current astronauts in space.

        Returns:
            Dictionary containing number of people in space and their names
            Returns None if request fails
        """
        try:
            response = requests.get(self.ISS_ASTROS_URL, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('message') == 'success':
                return {
                    'number': data['number'],
                    'people': data['people'],
                    'fetch_time': datetime.now().isoformat()
                }
        except requests.RequestException as e:
            print(f"Error fetching astronaut data: {e}")
            return None

        return None

    def calculate_velocity(self, lat1: float, lon1: float, lat2: float, lon2: float, time_diff: float) -> float:
        """
        Calculate approximate velocity between two positions.

        Args:
            lat1, lon1: First position coordinates
            lat2, lon2: Second position coordinates
            time_diff: Time difference in seconds

        Returns:
            Velocity in km/s
        """
        from math import radians, sin, cos, sqrt, atan2

        # Haversine formula to calculate distance
        R = 6371  # Earth radius in km

        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        distance = R * c

        if time_diff > 0:
            return distance / time_diff
        return 0.0


if __name__ == "__main__":
    # Test the fetcher
    fetcher = ISSDataFetcher()

    print("Testing ISS Data Fetcher...")
    print("\n1. Fetching current ISS position:")
    position = fetcher.fetch_position()
    if position:
        print(json.dumps(position, indent=2))

    print("\n2. Fetching astronauts in space:")
    astronauts = fetcher.fetch_astronauts()
    if astronauts:
        print(json.dumps(astronauts, indent=2))
