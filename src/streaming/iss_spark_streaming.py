"""
ISS Spark Streaming Application - Real-time analysis of ISS position data
Uses Spark Structured Streaming to process ISS data and generate insights
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, max, min, count, stddev,
    current_timestamp, to_timestamp, lit, when, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)
import os


class ISSSparkStreaming:
    """Real-time ISS data processing with Spark Structured Streaming."""

    def __init__(self, app_name: str = "ISS Real-Time Analysis"):
        """Initialize Spark session and define schema."""
        self.app_name = app_name
        self.spark = None
        self.schema = self._define_schema()

    def _define_schema(self) -> StructType:
        """Define schema for ISS position data."""
        return StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("timestamp", LongType(), True),
            StructField("fetch_time", StringType(), True),
            StructField("altitude_km", DoubleType(), True),
            StructField("velocity_km_s", DoubleType(), True),
            StructField("distance_to_paris_km", DoubleType(), True),
            StructField("orbit_phase", StringType(), True)

        ])

    def clean_checkpoints(self):
        """Clean checkpoint directories before starting."""
        import shutil
        checkpoint_base = "/opt/spark-data/checkpoints"

        if os.path.exists(checkpoint_base):
            print(f"Cleaning checkpoint directory: {checkpoint_base}")
            try:
                shutil.rmtree(checkpoint_base)
                os.makedirs(checkpoint_base, exist_ok=True)
                print("✓ Checkpoints cleaned successfully")
            except Exception as e:
                print(f"Warning: Could not clean checkpoints: {e}")

    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.streaming.checkpointLocation", "/opt/spark-data/checkpoints") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark

    def read_stream(self, input_path: str):
        """
        Read streaming data from JSON files.

        Args:
            input_path: Path to directory containing JSON files

        Returns:
            Streaming DataFrame
        """
        return self.spark.readStream \
            .schema(self.schema) \
            .json(input_path)

    def process_stream(self, df):
        """
        Process streaming data and add derived columns.

        Args:
            df: Input streaming DataFrame

        Returns:
            Processed streaming DataFrame
        """
        # Convert timestamp to proper datetime
        df = df.withColumn("event_time", to_timestamp(col("timestamp")))

        # Add hemisphere information
        df = df.withColumn("hemisphere_ns",
                          when(col("latitude") >= 0, "North").otherwise("South"))
        df = df.withColumn("hemisphere_ew",
                          when(col("longitude") >= 0, "East").otherwise("West"))

        # Round coordinates for readability
        df = df.withColumn("latitude", spark_round(col("latitude"), 4))
        df = df.withColumn("longitude", spark_round(col("longitude"), 4))
        df = df.withColumn("velocity_km_s", spark_round(col("velocity_km_s"), 4))

        return df

    def compute_statistics(self, df):
        """
        Compute windowed statistics on the stream.

        Args:
            df: Processed streaming DataFrame

        Returns:
            DataFrame with aggregated statistics
        """
        # Add watermark to handle late data (10 seconds tolerance)
        df_with_watermark = df.withWatermark("event_time", "10 seconds")

        # 1-minute windowed aggregations
        stats_df = df_with_watermark.groupBy(
            window(col("event_time"), "1 minute")
        ).agg(
            count("*").alias("data_points"),
            avg("latitude").alias("avg_latitude"),
            avg("longitude").alias("avg_longitude"),
            avg("velocity_km_s").alias("avg_velocity"),
            max("velocity_km_s").alias("max_velocity"),
            min("velocity_km_s").alias("min_velocity"),
            stddev("velocity_km_s").alias("velocity_stddev")
        )

        # Round aggregated values
        stats_df = stats_df.withColumn("avg_latitude", spark_round(col("avg_latitude"), 4))
        stats_df = stats_df.withColumn("avg_longitude", spark_round(col("avg_longitude"), 4))
        stats_df = stats_df.withColumn("avg_velocity", spark_round(col("avg_velocity"), 4))
        stats_df = stats_df.withColumn("max_velocity", spark_round(col("max_velocity"), 4))
        stats_df = stats_df.withColumn("min_velocity", spark_round(col("min_velocity"), 4))
        stats_df = stats_df.withColumn("velocity_stddev", spark_round(col("velocity_stddev"), 4))

        return stats_df

    def write_stream(self, df, output_path: str, query_name: str, output_mode: str = "append"):
        """
        Write streaming data to output.
        """
        checkpoint_path = f"/opt/spark-data/checkpoints/{query_name}"

        return df.writeStream \
            .outputMode(output_mode) \
            .format("json") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .option("cleanSource", "delete") \
            .queryName(query_name) \
            .start()

    def write_to_console(self, df, query_name: str, output_mode: str = "append"):
        """
        Write streaming data to console for debugging.

        Args:
            df: DataFrame to write
            query_name: Name for the streaming query
            output_mode: Output mode

        Returns:
            StreamingQuery object
        """

        return df.writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .queryName(query_name) \
            .option("truncate", "false") \
            .start()

    def run_analysis(self, input_path: str, output_path: str, console_output: bool = True):
        """
        Run the complete streaming analysis pipeline.

        Args:
            input_path: Path to input data directory
            output_path: Path to output directory
            console_output: Whether to output to console
        """
        print("=" * 80)
        print(f"ISS Real-Time Analysis with Apache Spark Structured Streaming")
        print("=" * 80)
        print(f"Input Path: {input_path}")
        print(f"Output Path: {output_path}")
        print(f"Checkpoint Location: /opt/spark-data/checkpoints")
        print("-" * 80)

        # Create Spark session
        self.create_spark_session()

        self.clean_checkpoints()

        # Read streaming data
        raw_stream = self.read_stream(input_path)

        # Process stream
        processed_stream = self.process_stream(raw_stream)

        # Compute statistics
        stats_stream = self.compute_statistics(processed_stream)

        # Start queries
        queries = []

        # Query 1: Write processed data
        query1 = self.write_stream(
            processed_stream,
            f"{output_path}/processed",
            "iss_processed_data",
            "append"
        )
        queries.append(query1)
        print("✓ Started query: iss_processed_data")

        # Query 2: Write statistics
        query2 = self.write_stream(
            stats_stream,
            f"{output_path}/statistics",
            "iss_statistics",
            "append"
        )
        queries.append(query2)
        print("✓ Started query: iss_statistics")

        # Query 3: Console output for monitoring
        if console_output:
            query3 = self.write_to_console(
                processed_stream.select(
                    "event_time", "latitude", "longitude",
                    "velocity_km_s", "distance_to_paris_km", "orbit_phase", "hemisphere_ns", "hemisphere_ew"
                ),
                "iss_console_output",
                "append"
            )
            queries.append(query3)
            print("✓ Started query: iss_console_output")

        print("-" * 80)
        print("Streaming queries are running... Press Ctrl+C to stop")
        print("=" * 80)

        # Wait for queries to finish
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\n\nStopping streaming queries...")
            for query in queries:
                query.stop()
            print("All queries stopped successfully")
        finally:
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='ISS Spark Streaming Application')
    parser.add_argument('--input', type=str, default='/opt/spark-data/raw',
                        help='Input directory path')
    parser.add_argument('--output', type=str, default='/opt/spark-data/processed',
                        help='Output directory path')
    parser.add_argument('--no-console', action='store_true',
                        help='Disable console output')

    args = parser.parse_args()

    # Create and run streaming application
    app = ISSSparkStreaming()
    app.run_analysis(
        input_path=args.input,
        output_path=args.output,
        console_output=not args.no_console
    )