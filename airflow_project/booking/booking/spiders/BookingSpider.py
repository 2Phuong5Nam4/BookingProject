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

    def __init__(self,  number_of_rooms=1, max_pages=0, *args, **kwargs):
        super(BookingAccommodationSpider, self).__init__(*args, **kwargs)

        with open("queries.json", "r") as file:
            self.queries = json.load(file)
        self.number_of_rooms = number_of_rooms
        self.max_pages = max_pages
        self.total_results = 0
        self.current_results = 0
        self.query_total_results = { query: 0 for query in self.queries }
        self.query_crawled_results = { query: set() for query in self.queries }
        self.hotel_ids = set()

    
    def get_graphql_body(self, response):
        body = {}
        # Get GraphQL body from the first page
        selector = Selector(response.text)
        script_data = selector.xpath(
            "//script[@data-capla-store-data='apollo']/text()").get()
        if not script_data:
            log.error(
                "No script data found with attribute data-capla-store-data='apollo'")
            body = {}
        else: 
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
        return body

    def start_requests(self):
        start_url = f"https://www.booking.com/searchresults.en-gb.html?"
        for query, params in self.queries.items():
            search_url = start_url + params
            log.debug(f"Constructed query: {query}")
            yield scrapy.Request(url=search_url, callback=self.parse_first_page, meta={"query": query})

    def parse_first_page(self, response):

        body = self.get_graphql_body(response)
        if not body:
            log.error("Failed to retrieve GraphQL body from the first page.")
            return
        try: 
            selector = Selector(response.text)
            _total_results = int(selector.css("h1").re(r"([\d,]+) properties found")[0].replace(",", ""))
            _max_scrape_results = self.max_pages * 25 if self.max_pages else _total_results
            if _max_scrape_results and _max_scrape_results < _total_results:
                _total_results = _max_scrape_results
            log.info(f"Query: {response.meta['query']}, Total results: {_total_results}")
            self.query_total_results[response.meta["query"]] = _total_results
            self.total_results += _total_results
            url = response.url
            url_params = url.split("?")[1]
            full_url = f"https://www.booking.com/dml/graphql?{url_params}"
            headers = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                "accept": "*/*",
                "cache-control": "no-cache",
                "content-type": "application/json",
                "origin": "https://www.booking.com",
                "pragma": "no-cache",
                "connection": "keep-alive",
                "priority": "u=1, i",
                "referer": url,
            }
            body["variables"]["input"]["pagination"]["rowsPerPage"] = 100
            for offset in range(0, _total_results, 100):
                body["variables"]["input"]["pagination"]["offset"] = offset
                yield scrapy.Request(
                    url=full_url,
                    method="POST",
                    headers=headers,
                    body=json.dumps(body),
                    callback=self.parse_graphql_response,
                    dont_filter=True,
                    meta={"query": response.meta["query"]}
                )
        except Exception as e:
            log.error(f"Error in parsing total results: {e}")

    def parse_graphql_response(self, response):
        try:
            data = json.loads(response.text)
            parsed_data = data["data"]["searchQueries"]["search"]["results"]
            for accommodation in parsed_data:
                id = accommodation["basicPropertyData"]["id"]
                if id in self.hotel_ids:
                    continue

                self.hotel_ids.add(id)
                self.query_crawled_results[response.meta["query"]].add(id)
                graphql_info = self.get_graphql_info(accommodation) 
                yield scrapy.Request(url=graphql_info["url"], 
                                     callback=self.parse_accommodation_url, 
                                     meta={"graphql_info": graphql_info,
                                            "query": response.meta["query"]
                                     })
        except KeyError as e:
            log.error(f"KeyError: {e} in JSON request: {response.url}")
        except json.JSONDecodeError as e:
            log.error(f"JSONDecodeError: {e} in response: {response.url}")


    def get_graphql_info(self, accommodation):
        id = accommodation["basicPropertyData"]["id"]
        name = accommodation["displayName"]["text"]
        typeId = accommodation["basicPropertyData"]['accommodationTypeId']
        description = accommodation["description"]["text"]
        star = accommodation["basicPropertyData"]["starRating"]["value"] if accommodation["basicPropertyData"]["starRating"] is not None else None
        reviewScore = accommodation["basicPropertyData"]["reviewScore"]["score"] if accommodation["basicPropertyData"]["reviewScore"] is not None else None
        reviewCount = accommodation["basicPropertyData"]["reviewScore"]["reviewCount"] if accommodation["basicPropertyData"]["reviewScore"] is not None else None
        accommodation_link = f"https://www.booking.com/hotel/vn/{accommodation["basicPropertyData"]["pageName"]}.en-gb.html" 
        city = accommodation["basicPropertyData"]["location"]["city"]               
        address = accommodation["basicPropertyData"]["location"]["address"]
        
        return {
            "id": id,
            "name": name,
            "typeId": typeId,
            "description": description,
            "star": star,
            "reviewScore": reviewScore,
            "reviewCount": reviewCount,
            "url": accommodation_link,
            "address": address + ", " + city
        }
    

    def parse_accommodation_url(self, response):
        accommodation = AccommodationItem()
        graphql_info = response.meta["graphql_info"]

        try:

            # Parse latitude and longitude
            try:
                lat, lng = response.css('[data-atlas-latlng]::attr(data-atlas-latlng)').get().split(",")
            except Exception as e:
                log.error(f"Error parsing latitude/longitude at {response.url}: {e}")
                        # Parse address


            # Parse facilities
            try:
                facilities_div = response.xpath('(//div[@data-testid="property-most-popular-facilities-wrapper"])[1]')
                facilities = facilities_div.xpath('.//ul/li')
                facilities_list = [" ".join(li.xpath('.//text()').getall()).strip() for li in facilities]
                if len(facilities_list) == 0:
                    log.warning(f"No facilities found for this accommodation {response.url}")
            except Exception as e:
                log.error(f"Error parsing facilities at {response.url}: {e}")

            # Parse check-in time
            try:
                property_section = response.xpath('//div[@data-testid="property-section--content"]')
                checkin = property_section.xpath('.//div[contains(.//div, "Check-in")]/following-sibling::div//text()').get()
            except Exception as e:
                log.error(f"Error parsing check-in time at {response.url}: {e}")

            # Parse check-out time
            try:
                checkout = property_section.xpath('.//div[contains(.//div, "Check-out")]/following-sibling::div//text()').get()
            except Exception as e:
                log.error(f"Error parsing check-out time at {response.url}: {e}")

            # Parse pet information
            try:
                pet_info = property_section.xpath('.//div[contains(.//div, "Pets") or contains(.//text(), "pet")]/following-sibling::div//text()').get()
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
            except Exception as e:
                log.error(f"Error parsing payment methods at {response.url}: {e}")


            url_parsed = {
                "lat": lat,
                "lng": lng,
                "unities": facilities_list,
                "checkinTime": checkin,
                "checkoutTime": checkout,
                "petInfo": pet_info,
                "paymentMethods": cards_accepted,
                "location": response.meta["query"]
            }
            accommodation.update(url_parsed)
            accommodation.update(graphql_info)
            self.current_results += 1
            log.info(f"Current results: {self.current_results}/{self.total_results}")
            yield accommodation

        except Exception as e:
            log.error(f"General error in parsing accommodation at {response.url}: {e}")
            yield accommodation

    def close(self, reason):
        log.info(f"Total results: {self.query_total_results}")
        log.info(f"Total unique hotel ids: {len(self.hotel_ids)}")
        for query, results in self.query_crawled_results.items():
            log.info(f"Query: {query}, Crawled results: {len(results)}/{self.query_total_results[query]}")





    
# Example usage
# if __name__ == "__main__":
#     process = CrawlerProcess(settings={
#         "LOG_LEVEL": "INFO",
#         "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
#     })
#     process.crawl(BookingSpider, query="Da Lat", checkin="2024-10-3", checkout="2024-10-4", number_of_rooms=1, max_pages=5)
#     process.start()
