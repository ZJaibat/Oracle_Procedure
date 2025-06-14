# ingest/spiders/nvidia_8k_json.py
import json, scrapy, re
from ingest.storage import save_raw

class Nvidia8KJsonSpider(scrapy.Spider):
    name = "nvidia_8k_json"
    start_urls = ["https://data.sec.gov/submissions/CIK0001045810.json"]
    custom_settings = {"DOWNLOAD_DELAY": 0.5}

    def parse(self, response):
        data = json.loads(response.text)
        recent = data["filings"]["recent"]
        for acc, form in zip(recent["accessionNumber"], recent["form"]):
            if form != "8-K":
                continue
            # build the folder URL once, no file name guessed
            acc_nodash = acc.replace("-", "")
            folder = (
                f"https://www.sec.gov/Archives/edgar/data/1045810/"
                f"{acc_nodash}/"
            )
            index_url = folder + "index.json"
            yield scrapy.Request(index_url,
                                 cb_kwargs={"folder": folder},
                                 callback=self.parse_index)

    def parse_index(self, response, folder):
        index = json.loads(response.text)
        # pick the primary 8-K document (ends with .htm or _doc.xml)
        for file in index["directory"]["item"]:
            name = file["name"]
            if re.search(r'8[-_]k.*\.(htm|xml)$', name, re.I):
                url = folder + name
                yield scrapy.Request(url, self.save_filing)
                break   # stop after first match

    def save_filing(self, response):
        path = save_raw(response.body, "xml", "nvidia_8k", response.url)
        self.logger.info("saved %s", path)

