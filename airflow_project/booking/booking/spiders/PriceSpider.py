import os
from datetime import datetime
import json
import scrapy
from scrapy.crawler import CrawlerProcess
from typing import Dict, List, Optional
from urllib.parse import urlencode
from loguru import logger as log
from parsel import Selector
import re
from booking.items import AccommodationItem, RoomPriceItem
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import json


class PriceSpider(scrapy.Spider):
    name = "price"
    def __init__(self, *args, **kwargs):
        super(PriceSpider, self).__init__(*args, **kwargs)
        self.current_date = datetime.now()
        self.current_date += timedelta(hours=7)
        self.max_range = [0, 1, 2, 5, 7, 14, 30]
        self.accommodation_urls = json.load(open("hotel_data/url.json"))
        # rename keys
        for i in range(len(self.accommodation_urls)):
            self.accommodation_urls[i]["id"] = self.accommodation_urls[i].pop("acm_id")
            self.accommodation_urls[i]["url"] = self.accommodation_urls[i].pop("acm_url")
        self.total_pages = len(self.accommodation_urls) * self.max_range
        self.current_pages = 0


    def start_requests(self):
        for accommodation in self.accommodation_urls:
            for i in self.max_range:
                checkin = self.current_date + timedelta(days=i)
                checkout = checkin + timedelta(days=1)
                # Parse the check-in date
                checkin_year, checkin_month, checkin_day = checkin.strftime("%Y-%m-%d").split("-") 
                checkout_year, checkout_month, checkout_day = checkout.strftime("%Y-%m-%d").split("-")  # Parse the check-out date
                url_params = urlencode(
                    {
                        "checkin_year": checkin_year,
                        "checkin_month": checkin_month,
                        "checkin_monthday": checkin_day,
                        "checkout_year": checkout_year,
                        "checkout_month": checkout_month,
                        "checkout_monthday": checkout_day,
                        "no_rooms": 1
                    }
                )
                search_url = f"{accommodation['url']}?{url_params}"
                yield scrapy.Request(url=search_url, 
                                    callback=self.parse_room_prices, 
                                    meta={"checkin": checkin.strftime("%Y-%m-%d"), "checkout": checkout.strftime("%Y-%m-%d"), "id": accommodation["id"]})
                

    def parse_room_prices(self, response):
        id = response.meta.get("id")
        checkin = response.meta.get("checkin")
        checkout = response.meta.get("checkout")
        
        try:
            table = response.css('table.hprt-table')
            rows = table.css('tbody').css('tr')
            for row in rows:
                number_of_columns = len(row.css('td'))
                if number_of_columns == 5:
                    item = RoomPriceItem()
                    item["accommodationId"] = id
                    item["checkin"] = checkin
                    item["checkout"] =  checkout
                    

                    cols = row.css('td')
                    item["roomId"] = cols[0].css('a.hprt-roomtype-link::attr(data-room-id)').get()
                    item["roomName"] = cols[0].css('span.hprt-roomtype-icon-link::text').get().strip()
                    item["price"] = row.attrib.get('data-hotel-rounded-price')

                    beds = cols[0].css('div.hprt-roomtype-bed').css('li span::text').getall()
                    # Strip the text and remove empty strings
                    item["beds"] = [bed.strip() for bed in beds if bed.strip()]

                    item["roomArea"] = cols[0].css('div.hprt-facilities-facility[data-name-en="room size"] span::text').get()
                    item["discount"] = cols[2].css('div[data-component="deals-container"] span.bui-badge__text::text').get()
                    item["numGuests"] = cols[1].css('span.bui-u-sr-only::text').get().strip()
                    item["url"] = response.url
                    yield item
            self.current_pages += 1
            log.success(f"Scraped {self.current_pages} out of {self.total_pages} pages")
        except Exception as e:
            log.error(f"Error in parsing room prices: {e} \n URL: {response.url}")
            return