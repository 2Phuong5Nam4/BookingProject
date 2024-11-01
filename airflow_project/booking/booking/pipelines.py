# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os
from datetime import datetime
from loguru import logger as log
import json


class BookingPipeline:
    def open_spider(self, spider):
        self.start_time = datetime.now()
        log.info(f"Starting at {self.start_time}")

    def close_spider(self, spider):
        end_time = datetime.now()
        log.info(f"Ending at {end_time}")
        # Calculate and log the total time taken in minutes
        hours, remainder = divmod((end_time - self.start_time).seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        log.info(f"Total time taken: {hours}h {minutes}m {seconds}s")

    def process_item(self, item, spider):
        return item  # Important to return the item for Scrapy

