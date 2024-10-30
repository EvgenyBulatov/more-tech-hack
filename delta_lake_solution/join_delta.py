from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import time
from delta import *

level = "*" #Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result" #Имя вашего бакета
your_access_key = "***" #Ключ от вашего бакета
your_secret_key = "***" #Ключ от вашего бакета

configs = {
    "spark.sql.files.maxPartitionBytes": "1073741824",  # 1GB
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.block.size": "134217728",  # 128MB
    "spark.hadoop.fs.s3a.multipart.size": "268435456",  # 256MB
    "spark.hadoop.fs.s3a.multipart.threshold": "536870912",  # 512MB
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
    "spark.sql.orc.compression.codec": "snappy",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", # подтягиваем delta-зависимости
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog", # подтягиваем delta-зависимости
    "spark.driver.memory": "6g",
    "spark.driver.cores": "1",
    "spark.executor.memory": "14g",
    "spark.executor.cores": "4",
    "spark.executor.instances": "8",
    "spark.default.parallelism": "56",
    "spark.sql.shuffle.partitions": "56"
}
conf = SparkConf()
conf.setAll(configs.items())

# создаем сессию
builder = SparkSession.builder.config(conf=conf)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext
log = spark._jvm.org.apache.log4j.LogManager.getLogger(">>> App")

# пути до источников
incr_bucket = f"s3a://source-data"
your_bucket = f"s3a://{your_bucket_name}"
incr_table = f"{incr_bucket}/incr{level}"  # таблица с источника ODS , куда мы скопировали инкремент
init_table = f"{your_bucket}/init{level}"  # реплика

start = time.perf_counter()

# таблицы
increment_table = spark.read.parquet(incr_table)
target_table = spark.read.format("delta").load(init_table)

target_columns = target_table.columns

# добавляем колонки партиционирования в приходящий инкремент
increment = (increment_table
             .withColumn("eff_from_month", last_day(col("eff_from_dt")))
             .withColumn("eff_to_month", last_day(col("eff_to_dt")))
             .repartition("eff_to_month", "eff_from_month")
             .selectExpr(target_columns)
             )
# по сути, новые данные с закрытой датой
increment_closed = increment \
    .filter(col("eff_to_month") != lit("5999-12-31")).select("id", "eff_from_month", "eff_from_dt").cache()

# берем список партиций из инкремента, так как именно по ним могут прийти апдейты
rows = increment_closed.select(col("eff_from_month") \
                                          .cast(StringType())).distinct().orderBy("eff_from_month").collect()

partitions = [str(row[0]) for row in rows]

# проходимся по партициям инкремента
for from_dt in partitions:
    log.info(f"<-start working with partition {from_dt}->")

    # берем только нужную партицию инита
    target_partition = target_table \
        .filter(col("eff_to_month") == lit("5999-12-31")) \
        .filter(col("eff_from_month") == from_dt)

    # закрытые записи инкремента
    closed_increment_partitioning = increment_closed \
        .filter(col("eff_from_month") == from_dt)

    # открытые записи без изменений
    old_data = target_partition.join(closed_increment_partitioning, on=["id", "eff_from_dt"], how='left_anti')

    old_data.write. \
        format("delta"). \
        mode("overwrite"). \
        partitionBy("eff_to_month", "eff_from_month"). \
        option("partitionOverwriteMode", "dynamic"). \
        save(init_table)

# аппендим оставшиеся данные инкремента в init
increment.write.format("delta").mode("append").save(init_table)

end = time.perf_counter() - start

log.info('{:.6f}s for the calculation'.format(end))