# -*- coding: utf-8 -*-
import scrapy
import re
from scrapy.http import Request
from urllib.parse import urljoin
from ArticleSpider.items import JobBoleArticleItem
from ArticleSpider.utils.common import get_md5
from datetime import datetime

# from scrapy.loader import ItemLoader
# from selenium import webdriver
# 关闭信号关闭浏览器
# from scrapy.xlib.pydispatch import dispatcher
# from scrapy import signals


class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']

    # 个性化配置
    custom_settings = {
        'ITEM_PIPELINES': {
            # 下载图片
            'ArticleSpider.pipelines.ArticleImagePipeline': 1,
            # 保存到mysql
            # 'ArticleSpider.pipelines.MysqlTwistedPipeline': 2, #
            # 保存到es
            'ArticleSpider.pipelines.ElasticsearchPipeline': 3,
        }
    }

    # 如果加载动态网页
    # def __init__(self):
    #     self.browser = webdriver.Chrome(executable_path="E:\phantomjs-2.1.1-windows\\bin\phantomjs.exe")
    #     super(JobboleSpider, self).__init__()
    #     dispatcher.connect(self.spider_close, signals.spider_closed)
    #
    # def spider_close(self, spider):
    #     # 爬虫退出
    #     print('spider closed')
    #     self.browser.quit()

    def parse(self, response):
        '''
        :param response:
        :return:
        '''

        post_nodes = response.xpath("//div[@id='archive']//div[contains(@class,'floated-thumb')]/div[@class='post-thumb']/a")
        for post_node in post_nodes:
            image_url = post_node.css("img::attr(src)").extract_first()
            post_url = post_node.css("::attr(href)").extract_first()
            yield Request(url=urljoin(response.url, post_url), callback=self.parse_detail, meta={"front_image_url": urljoin(response.url, image_url)})

        next_url = response.css(".next.page-numbers::attr(href)").extract_first("")
        if next_url:
            yield Request(url=urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):
        artile_item = JobBoleArticleItem()
        front_image_url = response.meta.get("front_image_url", "")
        title = response.xpath("//div[@class='entry-header']/h1/text()").extract()
        create_date = response.xpath("//div[@class='entry-meta']/p/text()").extract_first("").replace('·', ' ').strip()
        praise_nums = int(response.xpath("//span[contains(@class,'vote-post-up')]/h10/text()").extract_first(0))
        fav_nums = response.xpath("//span[contains(@class,'bookmark-btn')]/text()").extract_first("")
        result = re.match(".*\s(\d+)\s.*", fav_nums)
        if result:
            fav_nums = int(result.group(1))
        else:
            fav_nums = 0
        comment_nums = response.xpath("//a[@href='#article-comment']/span/text()").extract_first("")
        result = re.match(".*\s(\d+)\s.*", comment_nums)
        if result:
            comment_nums = int(result.group(1))
        else:
            comment_nums = 0
        content = response.xpath("//div[@class='entry']").extract()
        tag_list = response.xpath("//div[@class='entry-meta']/p/a/text()").extract()
        tag_list = [element for element in tag_list if not element.strip().endswith("评论")]
        tags = ",".join(tag_list)

        artile_item['url_object_id'] = get_md5(response.url)
        artile_item['title'] = title
        artile_item['url'] = response.url
        try:
            create_date = datetime.strptime(create_date, "%Y/%m/%d").date()
        except Exception as e:
            create_date = datetime.now().date()
        artile_item['create_date'] = create_date
        artile_item['front_image_url'] = [front_image_url]
        artile_item['praise_nums'] = praise_nums
        artile_item['comment_nums'] = comment_nums
        artile_item['fav_nums'] = fav_nums
        artile_item['tags'] = tags
        artile_item['content'] = content

        # 通过Itemloader加载item
        # item_loader = ItemLoader(item=artile_item,response=response)
        # item_loader.add_xpath('title', "//div[@class='entry-header']/h1/text()")
        # item_loader.add_value("front_image_url", response.meta.get("front_image_url", ""))
        # artile_item = item_loader.load_item()

        yield artile_item
