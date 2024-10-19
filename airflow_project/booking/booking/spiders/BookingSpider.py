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
from booking.items import  RoomPriceItem, AccommodationItem
from collections import defaultdict
from datetime import datetime, timedelta


class BookingAccommodationSpider(scrapy.Spider):
    name = "accommodation"
    # Override the list of HTTP status codes to handle

    def __init__(self, query="Viet Nam",  number_of_rooms=1, max_pages=100, *args, **kwargs):
        super(BookingAccommodationSpider, self).__init__(*args, **kwargs)
        self.query = query
        self.number_of_rooms = number_of_rooms
        self.max_pages = max_pages
        self.total_results = 0

    def start_requests(self):
        
        checking_year, checking_month, checking_day = ("", "", "")
        checkout_year, checkout_month, checkout_day = ("", "", "")  
        url_params = urlencode(
            {
                "ss": self.query,
                "checkin_year": checking_year,
                "checkin_month": checking_month,
                "checkin_monthday": checking_day,
                "checkout_year": checkout_year,
                "checkout_month": checkout_month,
                "checkout_monthday": checkout_day,
                "no_rooms": self.number_of_rooms
            }
        )
        search_url = f"https://www.booking.com/searchresults.en-gb.html?{url_params}"
        log.debug(f"Constructed URL: {search_url}")  # Log the encoded URL
        yield scrapy.Request(url=search_url, callback=self.parse_first_page)

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
                callback=self.parse_graphql_response
            )
    

    
    def parse_graphql_response(self, response):
        try:
            data = json.loads(response.text)
            parsed_data = data["data"]["searchQueries"]["search"]["results"]
            for accommodation in parsed_data:
                accommodationItem = AccommodationItem()
                accommodation_link = f"https://www.booking.com/hotel/vn/{accommodation["basicPropertyData"]["pageName"]}.en-gb.html"
                
                star = None
                reviewScore = None
                reviewCount = None
                

                if accommodation["basicPropertyData"]["starRating"] is not None:
                    star = accommodation["basicPropertyData"]["starRating"]["value"]
                if accommodation["basicPropertyData"]["reviewScore"] is not None:
                    reviewScore = accommodation["basicPropertyData"]["reviewScore"]["score"]
                    reviewCount = accommodation["basicPropertyData"]["reviewScore"]["reviewCount"]

                accommodationItem['id'] = accommodation["basicPropertyData"]["id"]
                accommodationItem['name'] = accommodation["displayName"]["text"]
                accommodationItem['typeId'] = accommodation["basicPropertyData"]['accommodationTypeId']
                accommodationItem['star'] = star
                accommodationItem['reviewScore'] = reviewScore
                accommodationItem['reviewCount'] = reviewCount
                accommodationItem['url'] = accommodation_link
                accommodationItem['description'] = accommodation["description"]["text"]

                yield scrapy.Request(url=accommodation_link, callback=self.parse_accommodation, meta={"accommodation": accommodationItem})
            self.total_results += len(parsed_data)
            log.success(f"Scraped {self.total_results} results from search pages")
        except KeyError as e:
            log.error(f"KeyError: {e} in JSON response: {response.text}")
        except json.JSONDecodeError as e:
            log.error(f"JSONDecodeError: {e} in response: {response.text}")

    def parse_accommodation(self, response):
        accommodation = response.meta["accommodation"]

        try:
            html = response.text
            sel = Selector(text=html)
            
            # Parse latitude and longitude
            try:
                lat, lng = response.css('[data-atlas-latlng]::attr(data-atlas-latlng)').get().split(",")
                accommodation["lat"] = lat
                accommodation["lng"] = lng
            except Exception as e:
                log.error(f"Error parsing latitude/longitude at {response.url}: {e}")

            # Parse facilities
            try:
                facilities_div = response.xpath('(//div[@data-testid="property-most-popular-facilities-wrapper"])[1]')
                facilities = facilities_div.xpath('.//ul/li')
                facilities_list = [" ".join(li.xpath('.//text()').getall()).strip() for li in facilities]
                accommodation["unities"] = facilities_list
                if len(facilities_list) == 0:
                    log.warning(f"No facilities found for this accommodation {response.url}")
            except Exception as e:
                log.error(f"Error parsing facilities at {response.url}: {e}")

            # Parse check-in time
            try:
                property_section = response.xpath('//div[@data-testid="property-section--content"]')
                checkin = property_section.xpath('.//div[contains(.//div, "Check-in")]/following-sibling::div//text()').get()
                accommodation["checkin"] = checkin
            except Exception as e:
                log.error(f"Error parsing check-in time at {response.url}: {e}")

            # Parse check-out time
            try:
                checkout = property_section.xpath('.//div[contains(.//div, "Check-out")]/following-sibling::div//text()').get()
                accommodation["checkout"] = checkout
            except Exception as e:
                log.error(f"Error parsing check-out time at {response.url}: {e}")

            # Parse pet information
            try:
                pet_info = property_section.xpath('.//div[contains(.//div, "Pets") or contains(.//text(), "pet")]/following-sibling::div//text()').get()
                accommodation["petInfo"] = pet_info
            except Exception as e:
                log.error(f"Error parsing pet information at {response.url}: {e}")

            # Parse payment methods
            try:
                cards_accepted = property_section.xpath(
                    './/div[contains(.//div, "Cards accepted") or contains(.//text(), "Accepted payment")]/following-sibling::div[1]'
                ).xpath('.//img/@alt').getall()
                cards_accepted.extend(property_section.xpath(
                    './/div[contains(.//div, "Cards accepted") or contains(.//text(), "Accepted payment")]/following-sibling::div[1]'
                ).xpath('.//text()').getall())

                if " " in cards_accepted:
                    cards_accepted = cards_accepted[:cards_accepted.index(" ")]

                if len(cards_accepted) == 0:
                    cards_accepted.append("Cash")
                
                accommodation["paymentMethods"] = cards_accepted
            except Exception as e:
                log.error(f"Error parsing payment methods at {response.url}: {e}")

            # Parse address
            try:
                address = response.css(".hp_address_subtitle::text").get()
                accommodation["address"] = address
            except Exception as e:
                log.error(f"Error parsing address at {response.url}: {e}")

            yield accommodation

        except Exception as e:
            log.error(f"General error in parsing accommodation at {response.url}: {e}")
            yield accommodation

    
# Example usage
# if __name__ == "__main__":
#     process = CrawlerProcess(settings={
#         "LOG_LEVEL": "INFO",
#         "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
#     })
#     process.crawl(BookingSpider, query="Da Lat", checkin="2024-10-3", checkout="2024-10-4", number_of_rooms=1, max_pages=5)
#     process.start()
