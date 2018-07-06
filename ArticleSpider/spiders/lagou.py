# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from ArticleSpider.items import LagouJobItem, LagouJobItemLoader
from ArticleSpider.utils.common import get_md5
import datetime


class LagouSpider(CrawlSpider):
    name = 'lagou'
    allowed_domains = ['www.lagou.com']
    start_urls = ['https://www.lagou.com/']
    cookie = {'user_trace_token': '20180704223710-bc32625d-7f97-11e8-be3d-525400f775ce',
              'LGUID': '20180704223710-bc3265e6-7f97-11e8-be3d-525400f775ce',
              'index_location_city': '%E5%85%A8%E5%9B%BD', 'SEARCH_ID': '687fe9d0309049a8b9ae4ee69688be3b',
              'JSESSIONID': 'ABAAABAABEEAAJAE09B303B2B7269FE682DACE4B987AFEF',
              'X_HTTP_TOKEN': '100ac859c37e034350a326c77bb54c88', 'ab_test_random_num': '0',
              '_gat': '1', '_putrc': 'B7113516E2B70C7A123F89F2B170EADC', 'login': 'true',
              'unick': '%E6%8B%89%E5%8B%BE%E7%94%A8%E6%88%B76862', 'showExpriedIndex': '1',
              'showExpriedCompanyHome': '1', 'showExpriedMyPublish': '1', 'hasDeliver': '0',
              'gate_login_token': '732f773718a72f0cb5862ca278c847c022a52461f9f7a77ef5b62a501cd86b01',
              '_gid': 'GA1.2.376316670.1530715031', '_ga': 'GA1.2.1755895252.1530715031',
              'Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6': '1530720089,1530720749,1530720771,1530721492',
              'Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6': '1530721975',
              'LGSID': '20180705000129-8313d340-7fa3-11e8-98e3-5254005c3644',
              'LGRID': '20180705003254-e70ae13b-7fa7-11e8-be45-525400f775ce'}
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS':
        {
            'Connection': 'keep-alive',
            'Host': 'www.lagou.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
        },
        'COOKIES_ENABLED': True,
        'DOWNLOAD_DELAY': 1

    }
    rules = (
        Rule(LinkExtractor(allow=("zhaopin/.*",)), follow=True),
        Rule(LinkExtractor(allow=("gongsi/j\d+.html",)), follow=True),
        Rule(LinkExtractor(allow=r'jobs/\d+.html'), callback='parse_job', follow=True),
    )

    def parse_start_url(self, response):
        return [scrapy.Request(url='https://www.lagou.com/', headers=self.custom_settings['DEFAULT_REQUEST_HEADERS'], cookies=self.cookie)]

    def parse_job(self, response):

        # 解析拉勾网的职位
        i = {}
        item_loader = LagouJobItemLoader(item=LagouJobItem(), response=response)
        item_loader.add_css("title", ".job-name::attr(title)")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("salary", ".job_request .salary::text")
        item_loader.add_xpath("job_city", "//*[@class='job_request']/p/span[2]/text()")
        item_loader.add_xpath("work_years", "//*[@class='job_request']/p/span[3]/text()")
        item_loader.add_xpath("degree_need", "//*[@class='job_request']/p/span[4]/text()")
        item_loader.add_xpath("job_type", "//*[@class='job_request']/p/span[5]/text()")

        item_loader.add_css("tags", '.position-label li::text')
        item_loader.add_css("publish_time", ".publish_time::text")
        item_loader.add_css("job_advantage", ".job-advantage p::text")
        item_loader.add_css("job_desc", ".job_bt div")
        item_loader.add_css("job_addr", ".work_addr")
        item_loader.add_css("company_name", "#job_company dt a img::attr(alt)")
        item_loader.add_css("company_url", "#job_company dt a::attr(href)")
        item_loader.add_value("crawl_time", datetime.datetime.now())

        return item_loader.load_item()
