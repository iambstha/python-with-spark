import os
import time
import psutil
import multiprocessing
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

DB_URL = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
DB_PROPERTIES = {
    "user": os.getenv('DB_USERNAME'),
    "password": os.getenv('DB_PASSWORD'),
    "driver": "org.postgresql.Driver"
}

schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True)
])

def parse_fixed_width(line):
    """ Parses fixed-width formatted lines into Row objects. """
    return Row(
        id=line[0:9].strip(),
        first_name=line[9:30].strip(),
        last_name=line[30:51].strip(),
        address=line[51:82].strip(),
        city=line[82:103].strip(),
        state=line[103:110].strip(),
        zip_code=line[110:115].strip().zfill(5)
    )

def startReadAndWrite(spark: SparkSession) -> None:
    print("✅ Spark read and write started")

    sc = spark.sparkContext
    file_path = "data/large_fixed_width_data_1gb.txt"

    file_size = os.path.getsize(file_path) / (1024 * 1024)
    print(f"📁 File Size: {file_size:.2f} MB")

    cpu_cores = multiprocessing.cpu_count()
    num_partitions = max(16, cpu_cores * 2)

    read_start_time = time.time()
    lines = sc.textFile(file_path, minPartitions=num_partitions)
    mappedLines = lines.map(parse_fixed_width)
    # df = spark.createDataFrame(mappedLines, schema).repartition(num_partitions)
    df = spark.createDataFrame(mappedLines, schema).repartition("state", "city")

    read_elapsed_time = time.time() - read_start_time
    print(f"📊 File read completed in {read_elapsed_time:.2f} seconds!")
    print(f"🛠️ Number of partitions in DataFrame: {df.rdd.getNumPartitions()}")

    write_start_time = time.time()
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "customer_data") \
        .option("user", DB_PROPERTIES["user"]) \
        .option("password", DB_PROPERTIES["password"]) \
        .option("driver", DB_PROPERTIES["driver"]) \
        .option("batchSize", "100000") \
        .option("numPartitions", str(num_partitions)) \
        .mode("append") \
        .save()

    write_elapsed_time = time.time() - write_start_time
    print(f"✅ Data written to PostgreSQL in {write_elapsed_time:.2f} seconds!")

    process = psutil.Process(os.getpid())
    memory_used = process.memory_info().rss / (1024 * 1024)
    cpu_percent = psutil.cpu_percent(interval=1)
    print(f"🖥️ Memory used: {memory_used:.2f} MB | CPU usage: {cpu_percent:.2f}%")

if __name__ == "__main__":
    print("Starting Spark session...")

    cpu_cores = multiprocessing.cpu_count()

    spark = SparkSession.builder \
        .appName("Optimized Read and Write") \
        .config("spark.jars", "lib/postgresql-42.7.5.jar") \
        .config("spark.sql.shuffle.partitions", str(cpu_cores * 2)) \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
        .master("local[*]") \
        .getOrCreate()

    print("✅ Spark session started")

    total_start_time = time.time()
    try:
        startReadAndWrite(spark)
    except Exception as e:
        print(f"❌ Error occurred: {e}")

    total_elapsed_time = time.time() - total_start_time
    print(f"✅ Total execution time: {total_elapsed_time:.2f} seconds!")

    spark.stop()
    print("✅ Spark session stopped")
