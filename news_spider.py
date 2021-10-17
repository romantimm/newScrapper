import scrapy
from datetime import datetime

class CveSpider(scrapy.Spider):
    name = 'cves_spider'
    allowed_domains = ['www.cvedetails.com']
    start_urls = []

    def __init__(self, products_list):
        base_url = 'https://www.cvedetails.com/product-search.php?vendor_id=0&search='
        for product in products_list:
            to_add = base_url + str(product).lower()
            self.start_urls.append(to_add)

    def parse(self, response):
        req_url = response.url
        err = response.xpath("//td[@class='errormsg']")
        if len(err) > 0:
            print("This: {} is not in cvedetails".format(req_url))
            return
        prod = req_url[str(req_url).find('&search=')+8:].capitalize()
        res = response.xpath("//a[text() = '{}']/@href".format(prod))
        url = 'https://www.cvedetails.com' + res.get()
        yield response.follow(url, callback=self.jump_to_vulnerabilities, meta={"keyword": prod})

    def jump_to_vulnerabilities(self, response):
        product = response.meta.get('keyword')
        res = response.xpath("//a[@title = 'Browse vulnerabilities of this product']/@href")
        url = 'https://www.cvedetails.com' + res.get()
        yield response.follow(url, callback=self.get_cves, meta={"keyword": product})

    def get_cves(self, response):
        keyword = response.meta.get('keyword')
        link = 'https://www.cvedetails.com'
        rows = response.xpath("//tr[@class='srrowns']")
        pages = len(response.xpath("//div[@class='paging']/*[@href]"))
        for row in rows:
            name = row.xpath("td[2]/a/text()").get()
            link = link + str(row.xpath("td[2]/a/@href").get())
            pub_date = row.xpath("td[6]/text()").get()
            yield {
                "link": link,
                "title": name,
                "date_published": pub_date,
                "date_found": datetime.now(),
                "keyword": keyword
            }
            link = 'https://www.cvedetails.com'

        for page in range(2, pages + 1):
            yield scrapy.Request("https://www.cvedetails.com/vulnerability-list.php?vendor_id=3578&product_id=8170"
                                 "&version_id=&page={}&hasexp=0&opdos=0&opec=0&opov=0&opcsrf=0&opgpriv=0&opsqli=0"
                                 "&opxss=0&opdirt=0&opmemc=0&ophttprs=0&opbyp=0&opfileinc=0&opginf=0&cvssscoremin=0"
                                 "&cvssscoremax=0&year=0&month=0&cweid=0&order=1&trc=166&sha"
                                 "=ff5124361ef40fcb477312259e1e4dcaf7808a37".format(page), callback=self.get_cves,
                                 meta={"keyword": keyword})

