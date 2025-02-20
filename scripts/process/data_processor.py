import os
import multiprocessing
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from helper.schema_def import schema
from config.db_config import DB_PROPERTIES, DB_URL
from utils.parser import parse_fixed_width
from utils.benchmark import Benchmark

def startReadAndWrite(spark: SparkSession) -> None:
    benchmark = Benchmark()
    print("✅ Spark read and write started")

    sc = spark.sparkContext
    # file_path = "data/large_fixed_width_data_1gb.txt"
    file_path = "data/data_file_sample.txt"

    file_size = os.path.getsize(file_path) / (1024 * 1024)
    print(f"📁 File Size: {file_size:.2f} MB")

    cpu_cores = multiprocessing.cpu_count()
    num_partitions = max(16, cpu_cores * 2)

    with benchmark.measure("File Read"):
        lines = sc.textFile(file_path, minPartitions=num_partitions)
        mappedLines = lines.map(parse_fixed_width)
        df = spark.createDataFrame(mappedLines, schema).repartition(num_partitions)
        # df.persist(StorageLevel.MEMORY_AND_DISK)
        df.printSchema()
        # df.select("first_name").show()
        # df.createOrReplaceTempView("customer_data")
        # df.show(5)
        # spark.sql("SELECT * FROM customer_data LIMIT 5").show()

    with benchmark.measure("NY Count Query"):
        df.createOrReplaceTempView("customer_data")
        # spark.sql("SELECT * FROM customer_data LIMIT 5").show()
        # ny_count_df = spark.sql("SELECT COUNT(*) as ny_count FROM customer_data WHERE state = 'NY'")
        # ny_count = ny_count_df.collect()[0]["ny_count"] if ny_count_df.count() > 0 else 0

    # print(f"🗽 Total number of people from NY: {ny_count}")

    # with benchmark.measure("Database Write Operation"):

    # df.write \
    #     .format("jdbc") \
    #     .option("url", DB_URL) \
    #     .option("dbtable", "customer_data") \
    #     .option("user", DB_PROPERTIES["user"]) \
    #     .option("password", DB_PROPERTIES["password"]) \
    #     .option("driver", DB_PROPERTIES["driver"]) \
    #     .option("batchSize", "100000") \
    #     .option("numPartitions", str(num_partitions)) \
    #     .mode("append") \
    #     .save()

    with benchmark.measure("System Resource Usage"):
        import psutil
        process = psutil.Process(os.getpid())
        memory_used = process.memory_info().rss / (1024 * 1024)
        cpu_percent = psutil.cpu_percent(interval=0.1)
        print(f"🖥️ Memory used: {memory_used:.2f} MB | CPU usage: {cpu_percent:.2f}%")

    # benchmark.summary()
