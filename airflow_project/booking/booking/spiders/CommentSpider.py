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
from booking.items import  RoomPriceItem, AccommodationItem, CommentItem
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd


class CommentSpider(scrapy.Spider):
    name = "comment"
    # Override the list of HTTP status codes to handle

    def __init__(self, *args, **kwargs):
        super(CommentSpider, self).__init__(*args, **kwargs)
        list_hotel = []
        with open("hotel_data/hotel_info.json", "r") as f:
            list_hotel = json.load(f)
        df = pd.DataFrame(list_hotel)
        # rename columns
        df = df.rename(columns={"acm_id": "id", "acm_location": "location", "acm_review_count": "reviewCount"})

        with open("queries.json", "r") as f:
            queries = json.load(f)
        df['destId'] = df['location'].apply(lambda x: int(queries[x].split("&")[1].split("=")[1]))
        df['destType'] = df['location'].apply(lambda x: queries[x].split("&")[2].split("=")[1])
        log.info(df.head())
        self.list_hotel = df
        self.total_results = df['reviewCount'].sum()
        self.current_results = 0

    def get_graphql_body(self, destId, destType, hotelId, offset):
        body = {}
        # Get GraphQL body from the first page
        try:
            with open("review_list.graphql", "r") as file:
                full_query = file.read()


            body = {
                "operationName": "ReviewList",
                "variables": {
                    "input": {
                        "filters": {
                            "text": ""
                        },
                        "hotelCountryCode": "vn",
                        "hotelId": hotelId,
                        "limit": 25,
                        "searchFeatures": {
                            "destId": destId,
                            "destType": destType
                        },
                        "skip": offset,
                        "sorter": "NEWEST_FIRST",
                        "ufi": destId,
                        "upsortReviewUrl": ""
                    }
                },
                "extensions": {},
                "query": full_query
            }
        except (KeyError, json.JSONDecodeError) as e:
            log.error(f"Error in retrieving GraphQL body: {e}")
            body = {}
        return body

    def start_requests(self):
        url = "https://www.booking.com/dml/graphql?lang=en-gb"
        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Host": "www.booking.com",
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        for index, row in self.list_hotel.iterrows():
            destId = row['destId']
            destType = row['destType']
            hotelId = row['id']
            reviewCount = row['reviewCount']

            for offset in range(0, reviewCount, 25):
                body = self.get_graphql_body(destId, destType, hotelId, offset)
                yield scrapy.Request(url=url,
                                        headers=headers,
                                        method="POST",
                                        body=json.dumps(body),
                                        callback=self.parse,
                                        meta={"hotelId": hotelId})

    def parse(self, response):
        try:
            data = json.loads(response.text)["data"]["reviewListFrontend"]["reviewCard"]
            hotelId = response.meta["hotelId"]
            for review in data:
                item = CommentItem()
                item['accommodationId'] = hotelId
                item['roomId'] = review['bookingDetails']['roomId']
                item['reviewedDate'] = review['reviewedDate']
                item['reviewUrl'] = review['reviewUrl']
                item['language'] = review['textDetails']['lang']
                item['title'] = review['textDetails']['title']
                item['positiveText'] = review['textDetails']['positiveText']
                item['negativeText'] = review['textDetails']['negativeText']
                item['numNights'] = review['bookingDetails']['numNights']
                item['stayStatus'] = review['bookingDetails']['stayStatus']
                item['customerType'] = review['bookingDetails']['customerType']
                item['checkinDate'] = review['bookingDetails']['checkinDate']
                item['guestCountry'] = review['guestDetails']['countryName']
                item['reviewScore'] = review['reviewScore']
                yield item
            self.current_results += len(data)
            log.info(f"Current results: {self.current_results}/{self.total_results}")
        except (KeyError, json.JSONDecodeError) as e:
            # log.error(f"Error in parsing response: {e}")
            log.error(f"Response: {response.url}")
            with open("error.json", "w") as f:
                f.write(json.dumps(response.text, indent=4))
        except Exception as e:
            log.error(f"Error in parsing response: {e}")
            log.error(f"Response: {response.url}")


    
    