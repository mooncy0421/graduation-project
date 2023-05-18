from konlpy.tag import Mecab

from pyspark.sql.types import Row, StringType
from pyspark.sql.functions import col, udf

from configs.get_spark import get_spark_session
from configs.get_logger import init_logger


LOGGER = init_logger(log_filename="airflow")

mecab = Mecab()

@udf(StringType())
def tokenize_raw_text(news_text):
    tokens = mecab.nouns(news_text)
    return " ".join(tokens)

class TokenizeNewsInfo():
    def __init__(self, task_config):
        self.task_config = task_config

        self.ingress_path = f"/lake/{self.task_config['ingress']['domain_name']}"

        self.elastic_host = self.task_config["egress"]["elastic_host"]
        self.egress_indice = self.task_config["egress"]["indice_name"]

    def __call__(self, context, snapshot_dt):
        LOGGER.info(f"===== START {__class__.__name__}  =====")

        source_path = f"{self.ingress_path}/{snapshot_dt}"

        spark = get_spark_session()
        news_info_df = spark.read.json(source_path)
        news_info_with_tokens_df = news_info_df.withColumn("tokens", tokenize_raw_text(col("news_text")))

        news_info_with_tokens_df.write\
            .option("es.nodes", self.elastic_host)\
            .option("es.resource", self.egress_indice)\
            .option("es.batch.write.refresh", "true")\
            .option("es.batch.write.preserve", "true")\
            .format("org.elasticsearch.spark.sql").mode("append").save(self.egress_indice)


import yaml
if __name__ == "__main__":
    task_config_path = "/home/hcy/workspace/graduation-project/builds/resources/dag.yml"
    with open(task_config_path, "r") as f:
        task_config = yaml.load(f, Loader=yaml.Loader)["dag_crawl_news_info"][2]
    TokenizeNewsInfo(task_config)(context=None, snapshot_dt="20230506_000000_000000")