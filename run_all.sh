#!/usr/bin/env bash
set -e
source .venv/bin/activate
scrapy crawl bis_fr_api
scrapy crawl nvidia_8k_json
scrapy crawl asml_pr

