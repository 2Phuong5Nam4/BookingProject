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
       
class JsonWriterPipeline:
    def open_spider(self, spider):
        self.start_time = datetime.now()
        log.info(f"Starting at {self.start_time}")

        # # Get the current date
        # self.current_date = datetime.now().strftime("%Y-%m-%d")

        # # Create a folder inside data folder with the current date as its name
        # os.makedirs("hotel_data", exist_ok=True)

        # # Dictionary to keep track of files for different item types
        # self.files = {}

    def close_spider(self, spider):
        # Close all open files
        # for file in self.files.values():
        #     # Remove the trailing comma and newline, then close the JSON array
        #     file.seek(file.tell() - 2, 0)
        #     file.truncate()
        #     file.write('\n]\n')
        #     file.close()
            
        end_time = datetime.now()
        log.info(f"Ending at {end_time}")
        # Calculate and log the total time taken in minutes
        hours, remainder = divmod((end_time - self.start_time).seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        log.info(f"Total time taken: {hours}h {minutes}m {seconds}s")


    def process_item(self, item, spider):
        # Determine the item type
        # item_type = type(item).__name__

         # If the file for this item type does not exist, create it
        # if item_type not in self.files:
        #     file = open(f'hotel_data/{self.current_date}-{item_type}.json', 'w', encoding='utf-8')
        #     self.files[item_type] = file
        #     file.write('[\n')  # Start the JSON array
        # else:
        #     file = self.files[item_type]

        # # Write the item to the appropriate file
        # line = json.dumps(dict(item), ensure_ascii=False) + ",\n"
        # file.write(line)
        return item  # Important to return the item for Scrapy

