"""
ISS Data Generator - Continuously fetches ISS data and writes to streaming source
This simulates a real-time data stream for Spark Structured Streaming
"""
import sys
import os
import time
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.iss_data_fetcher import ISSDataFetcher

from math import radians, sin, cos, sqrt, atan2


class ISSDataGenerator:
    """Generates continuous stream of ISS position data."""

    def __init__(self, output_dir: str, interval: int = 5):
        """
        Initialize the data generator.

        Args:
            output_dir: Directory to write data files
            interval: Time between data fetches in seconds
        """
        self.output_dir = output_dir
        self.interval = interval
        self.fetcher = ISSDataFetcher()
        self.previous_position = None

        self.paris_lat = 48.8566
        self.paris_lon = 2.3522

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def generate_stream(self, duration: int = 300):
        """
        Generate streaming data for a specified duration.

        Args:
            duration: How long to generate data in seconds (default 5 minutes)
        """
        print(f"Starting ISS data stream generator...")
        print(f"Writing to: {self.output_dir}")
        print(f"Fetch interval: {self.interval} seconds")
        print(f"Duration: {duration} seconds")
        print("-" * 60)

        start_time = time.time()
        data_points = 0

        try:
            while (time.time() - start_time) < duration:
                # Fetch current position
                position = self.fetcher.fetch_position()

                if position:
                    # Calculate velocity if we have previous position
                    if self.previous_position:
                        if position["latitude"] > self.previous_position["latitude"]:
                            position["orbit_phase"] = "ASCENDING"
                        else:
                            position["orbit_phase"] = "DESCENDING"

                        time_diff = position['timestamp'] - self.previous_position['timestamp']
                        velocity = self.fetcher.calculate_velocity(
                            self.previous_position['latitude'],
                            self.previous_position['longitude'],
                            position['latitude'],
                            position['longitude'],
                            time_diff
                        )
                        position['velocity_km_s'] = round(velocity, 4)
                    else:
                        position['velocity_km_s'] = 0.0
                        position["orbit_phase"] = "UNKNOWN"

                    # Distance to Paris
                    position["distance_to_paris_km"] = self.distance_to_paris(
                        position["latitude"], position["longitude"]
                    )

                    # Write to file (one file per data point for streaming)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    filename = f"iss_data_{timestamp}.json"
                    filepath = os.path.join(self.output_dir, filename)

                    with open(filepath, 'w') as f:
                        json.dump(position, f)

                    data_points += 1
                    print(f"[{data_points}] Position: ({position['latitude']:.4f}, {position['longitude']:.4f}) | "
                          f"Velocity: {position['velocity_km_s']:.4f} km/s | File: {filename}")

                    self.previous_position = position

                # Wait for next interval
                time.sleep(self.interval)

        except KeyboardInterrupt:
            print("\n\nStream generation stopped by user")

        elapsed = time.time() - start_time
        print("-" * 60)
        print(f"Stream generation completed!")
        print(f"Total data points: {data_points}")
        print(f"Elapsed time: {elapsed:.2f} seconds")
        print(f"Average rate: {data_points / elapsed:.2f} points/second")

    def distance_to_paris(self, lat, lon):
        R = 6371  # km
        lat1, lon1, lat2, lon2 = map(
            radians, [lat, lon, self.paris_lat, self.paris_lon]
        )
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return round(R * c, 2)


if __name__ == "__main__":
    # Default output directory
    output_dir = "/opt/spark-data/raw" if os.path.exists("/opt/spark-data") else "../../data/raw"

    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Generate ISS data stream')
    parser.add_argument('--output', type=str, default=output_dir,
                        help='Output directory for data files')
    parser.add_argument('--interval', type=int, default=5,
                        help='Seconds between data fetches (default: 5)')
    parser.add_argument('--duration', type=int, default=300,
                        help='Duration in seconds (default: 300 = 5 minutes)')

    args = parser.parse_args()

    # Create and run generator
    generator = ISSDataGenerator(args.output, args.interval)
    generator.generate_stream(args.duration)