import json, feedparser, io, scrapy
from ingest.storage import save_raw

class Nvidia8KSpider(scrapy.Spider):
    name = "nvidia_8k"
    custom_settings = {"DOWNLOAD_DELAY": 0.5}

    FEED_URL = (
        "https://www.sec.gov/cgi-bin/browse-edgar?"
        "action=getcurrent&type=8-K&CIK=1045810&count=40&output=atom"
    )

    def start_requests(self):
    	yield scrapy.Request(
        	self.FEED_URL,
        	self.parse_feed,
        	headers={"Accept": "application/atom+xml"},   # ← add this
        	dont_filter=True
    	)


    def parse_feed(self, response):
        feed = feedparser.parse(io.BytesIO(response.body))
        for entry in feed.entries:
            yield scrapy.Request(entry.link, self.save_filing)

    def save_filing(self, response):
        path = save_raw(response.body, "xml", "nvidia_8k", response.url)
        self.logger.info("saved %s", path)

