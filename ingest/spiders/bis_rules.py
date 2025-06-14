import scrapy, hashlib, datetime
from ingest.storage import save_raw


class BisRulesSpider(scrapy.Spider):
    name = "bis_rules"
    custom_settings = {"DOWNLOAD_DELAY": 1.0, "AUTOTHROTTLE_ENABLED": True}

    def __init__(self, SOURCE_URL, **kw):
        super().__init__(**kw)
        self.start_urls = [SOURCE_URL]
        self.source_id = "bis_rules"

    def parse(self, response):
        for href in response.css('a[href$=".pdf"]::attr(href)').getall():
            yield response.follow(href, self.save_pdf)

    def save_pdf(self, response):
        path = save_raw(response.body, "pdf", self.source_id, response.url)
        yield {"saved": path}

