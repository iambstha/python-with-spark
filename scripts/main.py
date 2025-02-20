
import os
import sys
import multiprocessing
from pyspark.sql import SparkSession
from process.data_processor import startReadAndWrite
from utils.test_db import checkDb
from utils.benchmark import Benchmark

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":

    benchmark = Benchmark()

    cpu_cores = multiprocessing.cpu_count()

    with benchmark.measure("Database Check"):
        checkDb()

    with benchmark.measure("Spark Session Initialization"):
        print("Starting Spark session...")
        spark = SparkSession.builder \
            .appName("Optimized Read and Write") \
            .config("spark.jars", "lib/postgresql-42.7.5.jar") \
            .config("spark.sql.shuffle.partitions", str(cpu_cores * 2)) \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
            .config("spark.sql.autoBroadcastJoinThreshold", -1) \
            .config("spark.memory.offHeap.enabled","true") \
            .config("spark.memory.offHeap.size","10g") \
            .master("local[*]") \
            .getOrCreate()

    print("✅ Spark session started")

    with benchmark.measure("Total Execution"):
        try:
            startReadAndWrite(spark)
        except Exception as e:
            print(f"❌ Error occurred: {e}")

    spark.stop()
    print("✅ Spark session stopped")

    # Print Benchmark Summary
    benchmark.summary()
