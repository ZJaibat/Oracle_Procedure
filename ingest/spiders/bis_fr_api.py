# ingest/spiders/bis_fr_api.py
import scrapy, json
from ingest.storage import save_raw

class BisFederalRegisterAPISpider(scrapy.Spider):
    name = "bis_fr_api"

    def start_requests(self):
        url = (
            "https://www.federalregister.gov/api/v1/documents.json"
            "?conditions[agency_ids]=451"
            "&conditions[type]=RULE"
            "&per_page=1000&order=newest"
        )
        yield scrapy.Request(url, self.parse_index, dont_filter=True)

    def parse_index(self, response):
        for doc in json.loads(response.text)["results"]:
            yield scrapy.Request(doc["pdf_url"], self.save_pdf)

    def save_pdf(self, response):
        path = save_raw(response.body, "pdf", "bis_fr_api", response.url)
        self.logger.info("saved %s", path)

