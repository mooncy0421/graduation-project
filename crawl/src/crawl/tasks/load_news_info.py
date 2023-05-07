import re
import time
import json
import requests
from bs4 import BeautifulSoup

from pyspark.sql.types import Row, StringType
from pyspark.sql.functions import col, udf

from configs.get_spark import get_spark_session
from configs.get_logger import init_logger

LOGGER = init_logger(log_filename="airflow")

@udf(StringType())
def parse_html(news_url):
    time.sleep(0.5)
    response = requests.get(
        news_url,
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    )
    parsed_news = BeautifulSoup(response.text, "html.parser")

    return str(parsed_news)

@udf(StringType())
def get_editor_name(parsed_news, title):
    parsed_news = BeautifulSoup(parsed_news)
    try:
        editor_name = parsed_news.find("em", "media_end_head_journalist_layer_name").text.strip(" 기자")
    except:
        editor_name = ""
        LOGGER.info(f"Can not parse EDITOR NAME from : {title}")
    return editor_name

@udf(StringType())
def get_agency_name(parsed_news, title):
    parsed_news = BeautifulSoup(parsed_news)
    try:
        agency_name = parsed_news.find("img", "media_end_head_top_logo_img light_type").get("alt")
    except:
        agency_name = ""
        LOGGER.info(f"Can not parse AGENCY NAME from : {title}")

    return agency_name

@udf(StringType())
def get_news_datetime(parsed_news, title):
    parsed_news = BeautifulSoup(parsed_news)
    try:
        news_datetime = parsed_news.find("div", "media_end_head_info_datestamp_bunch").find("span").get("data-date-time")    # 투고 일시
    except:
        news_datetime = ""
        LOGGER.info(f"Can not parse NEWS DATETIME from : {title}")
    return news_datetime

@udf(StringType())
def get_news_text(parsed_news, title):
    parsed_news = BeautifulSoup(parsed_news)
    try:
        news_text = re.sub("\n|[a-zA-Z0-9]+\@[a-z\.]+", "", parsed_news.find("div", "newsct_article _article_body").text).strip()    # 기사 원문
    except:
        news_text = ""
        LOGGER.info(f"Can not parse NEWS TEXT from : {title}")
    return news_text


class LoadNewsInfo():
    def __init__(self, task_config):
        self.task_config = task_config

        self.ingress_path = f"/lake/{self.task_config['ingress']['domain_name']}"
        self.egress_path = f"/lake/{self.task_config['egress']['domain_name']}"

    def __call__(self, context, snapshot_dt):
        LOGGER.info(f"===== START {__class__.__name__}  =====")

        spark = get_spark_session()
        url_title_df = spark.read.json(f"{self.ingress_path}/{snapshot_dt}")

        news_info_df = url_title_df.withColumn("news_html", parse_html(col("url")))\
                                    .withColumn("editor_name", get_editor_name(col("news_html"), col("title")))\
                                    .withColumn("agency_name", get_agency_name(col("news_html"), col("title")))\
                                    .withColumn("news_datetime", get_news_datetime(col("news_html"), col("title")))\
                                    .withColumn("news_text", get_news_text(col("news_html"), col("title"))).drop(col("news_html"))
        
        news_info_df.write.option("compression", "gzip").format("json").mode("overwrite").save(f"{self.egress_path}/{snapshot_dt}")

        LOGGER.info(f"===== END Parsing {news_info_df.count()} News =====")
        LOGGER.info(f"===== End {__class__.__name__}  =====")


import yaml
if __name__ == "__main__":
    task_config_path = "/home/hcy/workspace/graduation-project/builds/resources/dag.yml"
    with open(task_config_path, "r") as f:
        task_config = yaml.load(f, Loader=yaml.Loader)["dag_crawl_news_info"][1]
    LoadNewsInfo(task_config)(context=None, snapshot_dt="20230506_000000_000000")