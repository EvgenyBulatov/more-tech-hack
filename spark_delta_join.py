from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import time
from delta import *

level = "2"  # Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result"  # Имя вашего бакета
your_access_key = "***"  # Ключ от вашего бакета
your_secret_key = "***"  # Ключ от вашего бакета

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
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.access.key": your_access_key,
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.secret.key": your_secret_key,
    "spark.sql.orc.compression.codec": "snappy"
}
conf = SparkConf()
conf.setAll(configs.items())

# создаем сессию
spark = SparkSession.builder.config(conf=conf).getOrCreate()
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
target_delta_table = DeltaTable.forPath(spark, init_table)

incr_cnt = increment_table.count()
tgt_cnt = target_delta_table.count()

# добавляем колонки партиционирования в приходящий инкремент
target_columns = target_delta_table.toDF().columns
increment = (increment_table
    .withColumn("eff_from_month", last_day(col("eff_from_dt")).cast(StringType()))
    .withColumn("eff_to_month", last_day(col("eff_to_dt")).cast(StringType()))
    .repartition("eff_to_month", "eff_from_month")
    .selectExpr(target_columns)
)

(target_delta_table.alias('tgt') \
    .merge(
        increment.alias('incr'),
        "tgt.eff_to_month = '5999-12-31' \
        AND incr.eff_to_month = '5999-12-31' \
        AND incr.eff_from_month = tgt.eff_from_month \
        AND tgt.eff_from_dt = incr.eff_from_dt \
        AND tgt.id = incr.id"
    )
    .whenMatchedUpdate(
        set= {
            "tgt.eff_to_dt": "incr.eff_to_dt",
            "tgt.eff_to_month": "incr.eff_to_month",
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

new_tgt_count = target_delta_table.count()

target_delta_table.toDF().write.format("delta").mode("overwrite").save(init_table)

end = time.perf_counter() - start

log.info('{:.6f}s for the calculation'.format(end))
log.info(f'init count: {tgt_cnt}')
log.info(f'incr count: {incr_cnt}')
log.info(f'total count: {new_tgt_count}')