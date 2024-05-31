from typing import Iterable

import scrapy
from books.items import BooksItem
import re
from scrapy import Request


class BooksCrawlSpider(scrapy.Spider):
    name = "books_crawl"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/"]

    rating_mapping = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }

    def start_requests(self):
        yield scrapy.Request(url="https://books.toscrape.com/catalogue/category/books/default_15/index.html", callback=self.parse)

    def parse(self, response):
        book_crawl = response.xpath("//ol[@class='row']/li/article[@class='product_pod']")
        for book in book_crawl:
            item = BooksItem()
            item["title"] = book.xpath('.//h3/a/@title').get()
            item["img_url"] = response.urljoin(book.xpath('.//img[@class="thumbnail"]/@src').get())
            rating_text = book.xpath('.//p[contains(@class, "star-rating")]/@class').get().split()[-1]
            item["rating"] = self.rating_mapping.get(rating_text, -1)
            item["price"] = float(book.xpath('.//p[@class="price_color"]/text()').get()[1:])
            item["status"] = book.xpath('.//p[contains(@class,"instock")]/text()[normalize-space()]').get().strip()

            # Lấy nội dung bên trong từng quyển sách
            detail_url = book.xpath('.//div[@class="image_container"]/a/@href').get()
            if detail_url:
                request = scrapy.Request(url=response.urljoin(detail_url), callback=self.detailBooks)
                request.meta["item"] = item
                yield request

        # Lấy đường dẫn tới trang kế tiếp
        next_page = response.xpath('//li[@class="next"]/a/@href').get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse)

    # Hàm lấy nội dung bên trong
    def detailBooks(self, response):
        item = response.meta["item"]
        #description
        item["desc"] = response.xpath('//article[@class="product_page"]/p/text()').get()
        table = response.xpath('//tr/td/text()').getall()
        item["upc"] = table[0]
        item["product_type"] = table[1]
        item["price_excl"] = float(table[2][1:])
        item["price_incl"] = float(table[3][1:])
        item["tax"] = float(table[4][1:])
        # item["availability"] = table[5]
        # Lấy số lượng sách còn lại
        match = re.search(r'\d+', table[5])
        item["availability"] = int(match.group())
        item["number_of_reviews"] = int(table[6])
        item["type_of_book"] = response.xpath("//ul[@class='breadcrumb']/li[3]/a/text()").get()
        yield item
