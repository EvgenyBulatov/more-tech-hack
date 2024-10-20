from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

#Вы только не расслабляйтесь, если решение вы будете подгонять под этот скрипт, это не гарантирует очки за корректность работы

level = "*" #Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result" #Имя вашего бакета
your_access_key = "***" #Ключ от вашего бакета
your_secret_key = "***" #Ключ от вашего бакета

configs = {
    "spark.sql.files.maxPartitionBytes": "1073741824", #1GB
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.block.size": "134217728", # 128MB
    "spark.hadoop.fs.s3a.multipart.size": "268435456", # 256MB
    "spark.hadoop.fs.s3a.multipart.threshold": "536870912", # 512MB
    "spark.hadoop.fs.s3a.committer.name": "magic",
    "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": "true",
    "spark.hadoop.fs.s3a.threads.max": "64",
    "spark.hadoop.fs.s3a.connection.maximum": "64",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "array",
    "spark.hadoop.fs.s3a.directory.marker.retention": "keep",
    "spark.hadoop.fs.s3a.endpoint": "api.s3.az1.t1.cloud",
    "spark.hadoop.fs.s3a.bucket.source-data.access.key": "P2EGND58XBW5ASXMYLLK",
    "spark.hadoop.fs.s3a.bucket.source-data.secret.key": "IDkOoR8KKmCuXc9eLAnBFYDLLuJ3NcCAkGFghCJI",
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.access.key": your_access_key,
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.secret.key": your_secret_key,
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
}
conf = SparkConf()
conf.setAll(configs.items())

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
log = spark._jvm.org.apache.log4j.LogManager.getLogger(">>> App")

incr_bucket = f"s3a://source-data"
your_bucket = f"s3a://{your_bucket_name}"
incr_table_path = f"{incr_bucket}/incr{level}"
init_table_path = f"{incr_bucket}/init{level}"
your_table_path = f"{your_bucket}/init{level}"

init_table = spark.read.parquet(init_table_path)
incr_table = spark.read.parquet(incr_table_path)
your_table = spark.read.format("delta").load(your_table_path)

init_line_count = init_table.count()
incr_new_lines_count = incr_table.where(col("id") > init_line_count).count()
closed_line_incr = incr_table.where(col("eff_to_dt") != "5999-12-31").count()

result_opened_lines = your_table.where(col("eff_to_month") == "5999-12-31").count()
closed_line_result = your_table.where(col("eff_to_month") != "5999-12-31").count()

if (init_line_count + incr_new_lines_count) == result_opened_lines:
    print(f"Open records match: {result_opened_lines}")
else:
    print(f"ERROR: Expected open records: {init_line_count + incr_new_lines_count}, actual: {result_opened_lines}")

if closed_line_incr == closed_line_result:
    print(f"Closed records match: {closed_line_result}")
else:
    print(f"ERROR: Expected closed records: {closed_line_incr}, actual: {closed_line_result}")
