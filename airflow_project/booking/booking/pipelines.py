# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os
from datetime import datetime
from loguru import logger as log
       
class CsvWriterPipeline:
    def open_spider(self, spider):
        self.start_time = datetime.now()
        log.info(f"Starting at {self.start_time}")

        # Get the current date
        self.current_date = datetime.now().strftime("%Y-%m-%d")

        # Create a folder inside data folder with the current date as its name
        os.makedirs("hotel_data", exist_ok=True)

        # Dictionary to keep track of files for different item types
        self.files = {}

    def close_spider(self, spider):
        # Close all open files
        for file in self.files.values():
            file.close()
        
        end_time = datetime.now()
        log.info(f"Ending at {end_time}")
        # Calculate and log the total time taken in minutes
        log.info(f"Total time taken in minutes: {(end_time - self.start_time).total_seconds() / 60}")

    def process_item(self, item, spider):
        # Determine the item type
        item_type = type(item).__name__

        # If the file for this item type does not exist, create it
        if item_type not in self.files:
            current_date = datetime.now().strftime("%Y-%m-%d")
            file = open(f'hotel_data/{self.current_date}-{item_type}.csv', 'w', newline='')
            self.files[item_type] = file
            fieldnames = item.keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
        else:
            file = self.files[item_type]

        # Write the item to the appropriate file
        writer = csv.DictWriter(file, fieldnames=item.keys())
        writer.writerow(item)

