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



class PriceSpider(scrapy.Spider):
    name = "price"
    def __init__(self, *args, **kwargs):
        super(PriceSpider, self).__init__(*args, **kwargs)
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.max_range = 30
        self.query = "Viet Nam"
        self.number_of_rooms = 1
        self.max_pages = 2
        self.total_pages_scraped = 0

    def start_requests(self):
        for i in range(self.max_range):
            checkin = datetime.now() + timedelta(days=i)

            checkout = checkin + timedelta(days=1)
            # Parse the check-in date
            checkin_year, checking_month, checking_day = checkin.strftime("%Y-%m-%d").split("-") 
            checkout_year, checkout_month, checkout_day = checkout.strftime("%Y-%m-%d").split("-")  # Parse the check-out date
            url_params = urlencode(
                {
                    "ss": self.query,
                    "checkin_year": checkin_year,
                    "checkin_month": checking_month,
                    "checkin_monthday": checking_day,
                    "checkout_year": checkout_year,
                    "checkout_month": checkout_month,
                    "checkout_monthday": checkout_day,
                    "no_rooms": self.number_of_rooms
                }
            )
            search_url = f"https://www.booking.com/searchresults.en-gb.html?{url_params}"
            # log.debug(f"Constructed checkin: {checkin} checkout: {checkout} \n URL: {search_url}")  # Log the encoded URL
            yield scrapy.Request(url=search_url, callback=self.parse_first_page, meta={"checkin": checkin.strftime("%Y-%m-%d"), "checkout": checkout.strftime("%Y-%m-%d")})

                    

    def parse_first_page(self, response):
        body = {}

        # Get GraphQL body from the first page
        selector = Selector(response.text)
        script_data = selector.xpath(
            "//script[@data-capla-store-data='apollo']/text()").get()
        if not script_data:
            log.error(
                "No script data found with attribute data-capla-store-data='apollo'")
            body = {} 
    
        try:
            json_script_data = json.loads(script_data)
            keys_list = list(
                json_script_data["ROOT_QUERY"]["searchQueries"].keys())
            second_key = keys_list[1]
            search_query_string = second_key[len("search("):-1]
            input_json_object = json.loads(search_query_string)

            with open("search_query.graphql", "r") as file:
                full_query = file.read()

            body = {
                "operationName": "FullSearch",
                "variables": {
                    "input": input_json_object["input"],
                    "carouselLowCodeExp": False
                },
                "extensions": {},
                "query": full_query
            }
        except (KeyError, json.JSONDecodeError) as e:
            log.error(f"Error in retrieving GraphQL body: {e}")
            body = {}

        if not body:
            log.error("Failed to retrieve GraphQL body from the first page.")
            return
        _total_results = int(selector.css("h1").re(
            r"([\d,]+) properties found")[0].replace(",", ""))
        _max_scrape_results = self.max_pages * 25 if self.max_pages else _total_results
        if _max_scrape_results and _max_scrape_results < _total_results:
            _total_results = _max_scrape_results

        for offset in range(0, _total_results, 25):
            url = response.url
            body["variables"]["input"]["pagination"]["offset"] = offset
            url_params = url.split("?")[1]
            full_url = f"https://www.booking.com/dml/graphql?{url_params}"
            headers = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                "accept": "*/*",
                "cache-control": "no-cache",
                "content-type": "application/json",
                "origin": "https://www.booking.com",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "referer": url,
            }
            yield scrapy.Request(
                url=full_url,
                method="POST",
                headers=headers,
                body=json.dumps(body),
                callback=self.parse_graphql_response,
                meta={"checkin": response.meta["checkin"], "checkout": response.meta["checkout"]}
            )
    
    
    def parse_graphql_response(self, response):
        checkin = response.meta["checkin"]
        checkout = response.meta["checkout"]
        try:
            data = json.loads(response.text)
            parsed_data = data["data"]["searchQueries"]["search"]["results"]

           
           
            for accommodation in parsed_data:
                priceItem = RoomPriceItem()
                accommodation_id = accommodation["basicPropertyData"]["id"]
                priceItem["accommodationId"] = accommodation_id
                priceItem["checkin"] = checkin
                priceItem["checkout"] = checkout
                

                checking_year, checking_month, checking_day = checkin.split("-")
                checkout_year, checkout_month, checkout_day = checkout.split("-")
                url_params = urlencode(
                        {
                            "checkin_year": checking_year,
                            "checkin_month": checking_month,
                            "checkin_monthday": checking_day,
                            "checkout_year": checkout_year,
                            "checkout_month": checkout_month,
                            "checkout_monthday": checkout_day,
                            "no_rooms": self.number_of_rooms
                        } 
                )
                priceItem["url"] = f"https://www.booking.com/hotel/vn/{accommodation['basicPropertyData']['pageName']}.en-gb.html?{url_params}"
                yield scrapy.Request(
                    url=f"https://www.booking.com/hotel/vn/{accommodation['basicPropertyData']['pageName']}.en-gb.html?{url_params}",
                    callback=self.parse_room_prices, meta={"priceItem": priceItem }
                )
            self.total_pages_scraped += len(parsed_data)
            log.success(f"Scraped {self.total_pages_scraped} results from search pages")
        except KeyError as e:
            log.error(f"KeyError: {e} in JSON response: ")
        except json.JSONDecodeError as e:
            log.error(f"JSONDecodeError: {e} in response: ")
        except Exception as e:
            # Resend the request
            log.error(f"Error in parsing GraphQL response: {e}")
            with open("error.json", "w") as file:
                file.write(response.text)
            


    def parse_room_prices(self, response):
        priceItem = response.meta["priceItem"]

        try:
            table = response.css('table.hprt-table')
            rows = table.css('tbody').css('tr')
            for row in rows:
                number_of_columns = len(row.css('td'))
                if number_of_columns == 5:
                    item = RoomPriceItem()
                    item["accommodationId"] = priceItem["accommodationId"]
                    item["checkin"] = priceItem["checkin"]
                    item["checkout"] = priceItem["checkout"]
                    

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
                    item["url"] = priceItem["url"]
                    yield item
                    
        except Exception as e:
            log.error(f"Error in parsing room prices: {e}")
            return