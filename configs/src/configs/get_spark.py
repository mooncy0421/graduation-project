import json

from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session():
    with open("/home/hcy/workspace/credentials/config.json", mode="r") as f:
        configs = json.loads(f.read())

    gcs_keypath = "/home/hcy/workspace/credentials/gcp-credential.json"

    hadoop_config = {
        'fs.gs.auth.service.account.enable': 'true',
        'google.cloud.auth.service.account.json.keyfile': gcs_keypath,
    }

    spark_conf = SparkConf()
    default_spark_config = {
        "spark.driver.maxResultSize": "4g",
        "spark.master": "local[2]",
        "spark.sql.broadcastTimeout": "720000",
        "spark.rpc.lookupTimeout": "600s",
        "spark.network.timeout": "600s",
        'spark.hadoop.fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
        'spark.jars': f'file:////home/hcy/workspace/credentials/gcs-connector-hadoop2-latest.jar,file:////home/hcy/workspace/credentials/elasticsearch-spark-20_2.12-8.6.0.jar'
    }

    spark_conf_kvs = [(k, v) for k, v in default_spark_config.items()]

    spark_conf.setAll(spark_conf_kvs)
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.defaultFS", f"{configs['gcs.protocol']}{configs['gcs.bucketname']}/"
    )

    for k, v in hadoop_config.items():
        spark.sparkContext._jsc.hadoopConfiguration().set(k, v)

    return spark