# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AccommodationItem(scrapy.Item):
    # define the fields for your item here like:
    id = scrapy.Field()
    name = scrapy.Field()
    typeId = scrapy.Field()
    unities = scrapy.Field()
    description = scrapy.Field()
    address = scrapy.Field()
    lat = scrapy.Field()
    lng = scrapy.Field()
    reviewScore = scrapy.Field()
    reviewCount = scrapy.Field()
    url = scrapy.Field()
    star = scrapy.Field()
    checkinTime = scrapy.Field()
    checkoutTime = scrapy.Field()
    paymentMethods = scrapy.Field()
    petInfo = scrapy.Field()
    location = scrapy.Field()
    pass

class RoomPriceItem(scrapy.Item):
    # define the fields for your item here like:
    accommodationId = scrapy.Field()
    roomId = scrapy.Field()
    roomName = scrapy.Field()
    checkin = scrapy.Field()
    checkout = scrapy.Field()
    numGuests = scrapy.Field()
    roomArea = scrapy.Field()
    discount = scrapy.Field()
    beds = scrapy.Field()
    price = scrapy.Field()
    url = srcapy.Field()
    pass
