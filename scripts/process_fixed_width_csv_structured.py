import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2
import os
import glob
import shutil

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "test_db"
DB_USER = "bishal"
DB_PASSWORD = "shrestha"
TABLE_NAME = "customer_data"

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
    return (
        line[0:9].strip(),
        line[9:30].strip(),
        line[30:51].strip(),
        line[51:82].strip(),
        line[82:103].strip(),
        line[103:110].strip(),
        line[110:115].strip(),
    )

start_time = time.time()

spark = SparkSession.builder \
    .appName("FixedWidthToPostgres") \
    .getOrCreate()

file_path = "data/large_fixed_width_data_1gb.txt"
rdd = spark.sparkContext.textFile(file_path)

total_records = rdd.count()
batch_size = 20000
num_batches = (total_records // batch_size) + 1

print(f"📦 Processing {total_records} records in {num_batches} batches of {batch_size} each.")

conn = psycopg2.connect(
    host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
)
cur = conn.cursor()

for i in range(num_batches):
    batch_rdd = rdd.zipWithIndex().filter(lambda x: i * batch_size <= x[1] < (i + 1) * batch_size).map(lambda x: x[0])
    batch_df = spark.createDataFrame(batch_rdd.map(parse_fixed_width), schema)

    temp_dir = f"/tmp/customer_data_batch_{i}"
    batch_df.coalesce(1).write.csv(temp_dir, mode="overwrite", header=False)

    csv_files = glob.glob(f"{temp_dir}/*.csv")
    if not csv_files:
        print(f"❌ No CSV file found in {temp_dir}")
        continue

    temp_csv_path = csv_files[0]

    with open(temp_csv_path, "r") as f:
        cur.copy_from(f, TABLE_NAME, sep=",", columns=[c.name for c in schema])

    conn.commit()

    os.remove(temp_csv_path)
    shutil.rmtree(temp_dir)
    print(f"✅ Batch {i+1}/{num_batches} processed.")

cur.close()
conn.close()

elapsed_time = time.time() - start_time
print(f"🚀 Data successfully written to PostgreSQL in {elapsed_time:.2f} seconds!")

spark.stop()
