import re
import time
import requests
from bs4 import BeautifulSoup

from pyspark.sql.types import Row

from configs.get_spark import get_spark_session
from configs.get_logger import init_logger

LOGGER = init_logger(log_filename="airflow")


class LoadNewsUrlTitle():
    def __init__(self, task_config):
        self.task_config = task_config

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
        }
        self.ingress_urls = self.task_config["ingress"]["urls"]

        self.egress_path = f"/lake/{self.task_config['egress']['domain_name']}"

    def __call__(self, context, snapshot_dt):
        LOGGER.info(f"===== START {__class__.__name__}  =====")
        LOGGER.info("===== Parsing News url and title... =====")

        save_path = f"{self.egress_path}/{snapshot_dt}"
        snapshot_date = snapshot_dt[:8]

        spark = get_spark_session()

        url_title = []
        for url in self.ingress_urls:
            time.sleep(0.5)
            response = requests.get(url+snapshot_date, headers=self.headers)
            parsed_web = BeautifulSoup(response.text, "html.parser")

            news_list = parsed_web.find("div", "list_body newsflash_body").find_all("a")

            for news in news_list:
                if news.text.strip() and not news.find("img"):
                    news_pair = dict()
                    news_pair["url"] = news.get("href")
                    news_pair["title"] = news.text.strip()
                    url_title.append(Row(**news_pair))
        url_title = list({n["url"]: n for n in url_title}.values())

        url_title_df = spark.createDataFrame(url_title)
        url_title_df.write.option('compression', 'gzip').format('json').mode('overwrite').save(save_path)

        LOGGER.info(f"===== End Parsing {len(url_title)} News url and title... =====")
        LOGGER.info(f"===== End {__class__.__name__}  =====")


import yaml
if __name__ == "__main__":
    task_config_path = "/home/hcy/workspace/graduation-project/builds/resources/dag.yml"
    with open(task_config_path, "r") as f:
        task_config = yaml.load(f, Loader=yaml.Loader)["dag_crawl_news_info"][0]
    LoadNewsUrlTitle(task_config)(context=None, snapshot_dt="20230506_000000_000000")