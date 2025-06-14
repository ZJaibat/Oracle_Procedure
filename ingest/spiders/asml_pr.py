import scrapy
from ingest.storage import save_raw

class AsmlPRSpider(scrapy.Spider):
    name = "asml_pr"
    start_urls = ["https://www.asml.com/en/news/press-releases"]
    custom_settings = {"DOWNLOAD_DELAY": 0.5}

    def parse(self, response):
        # grab every anchor whose href contains the press-release slug
        for href in response.xpath('//a[contains(@href,"/en/news/press-releases/")]/@href').getall():
            # ensure absolute URL
            url = response.urljoin(href)
            yield scrapy.Request(url, self.save_html)

    def save_html(self, response):
        path = save_raw(response.text.encode(), "html", "asml_pr", response.url)
        self.logger.info("saved %s", path)

