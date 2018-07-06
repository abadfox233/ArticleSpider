# -*- coding: utf-8 -*-
import scrapy
from http import cookiejar
from requests.utils import dict_from_cookiejar
from urllib.parse import urljoin
import re
from scrapy.loader import ItemLoader
from ArticleSpider.items import ZhihuAnswerItem,ZhihuQuesionItem
import json
import datetime


class ZhihuSpider(scrapy.Spider):
    name = 'zhihu'
    allowed_domains = ['www.zhihu.com']
    start_urls = ['http://www.zhihu.com/']

    custom_settings = {
        'COOKIES_ENABLED': True,
        'DOWNLOAD_DELAY': 1.5
    }

    headers = {
        "HOST": "www.zhihu.com",
        "Referer": "https://www.zhizhu.com",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
    }

    start_answer_url = 'https://www.zhihu.com/api/v4/questions/{0}/answers?sort_by=default&include=data%5B%2A%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B%2A%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F%28type%3Dbest_answerer%29%5D.topics&limit={1}&offset={2}'

    def parse(self, response):
        all_urls = response.css("a::attr(href)").extract()
        all_urls = [urljoin(response.url, url) for url in all_urls]
        all_urls = filter(lambda x: True if x.startswith("https") else False, all_urls)
        for url in all_urls:
            match_obj = re.match("(.*zhihu\.com/question/(\d+))(/|$).*", url)
            if match_obj:
                request_url = match_obj.group(1)
                question_id = match_obj.group(2)
                yield scrapy.Request(request_url, headers=self.headers, callback=self.parse_question, meta={"question_id":question_id})
            else:
                # pass
                yield scrapy.Request(url, headers=self.headers, callback=self.parse)

    def parse_question(self, response):
        item_loader = ItemLoader(item=ZhihuQuesionItem(), response=response)
        item_loader.add_css('title', "h1.QuestionHeader-title::text")
        item_loader.add_css('content', '.QuestionHeader-detail')
        item_loader.add_value('url', response.url)
        item_loader.add_value('zhihu_id', response.meta.get('question_id'))
        item_loader.add_css('answer_num', '.List-headerText span::text')
        item_loader.add_css('comments_num',  '.QuestionHeader-Comment button::text')
        item_loader.add_css('watch_user_num', '.NumberBoard-itemValue::text')
        item_loader.add_css('topics', '.QuestionHeader-topics .Popover div::text')

        question_item = item_loader.load_item()
        yield scrapy.Request(self.start_answer_url.format(response.meta.get('question_id'), 20, 0),
                             headers=self.headers, callback=self.parse_answer)
        yield question_item

    def parse_answer(self, response):
        ans_json = json.loads(response.text)
        is_end = ans_json['paging']['is_end']
        next_url = ans_json['paging']['next']

        for answer in ans_json['data']:
            answer_item = ZhihuAnswerItem()
            answer_item["zhihu_id"] = answer["id"]
            answer_item["url"] = answer["url"]
            answer_item["question_id"] = answer["question"]["id"]
            answer_item["author_id"] = answer["author"]["id"] if "id" in answer["author"] else None
            answer_item["content"] = answer["content"] if "content" in answer else None
            answer_item["praise_num"] = answer["voteup_count"]
            answer_item["comments_num"] = answer["comment_count"]
            answer_item["create_time"] = answer["created_time"]
            answer_item["update_time"] = answer["updated_time"]
            answer_item["crawl_time"] = datetime.datetime.now()

            yield answer_item

        if not is_end:
            yield scrapy.Request(next_url, headers=self.headers, callback=self.parse_answer)

    def start_requests(self):
        '''
        加载cookie 保持登陆状态
        :return:
        '''
        cookies = cookiejar.LWPCookieJar(filename='./ArticleSpider/cookie.txt')
        try:
            cookies.load(ignore_discard=True)
        except FileNotFoundError:
            print("error")
        cookie_dic = dict_from_cookiejar(cookies)
        return [scrapy.Request(url='http://www.zhihu.com/', headers=self.headers, cookies=cookie_dic, callback=self.second)]

    def second(self, response):
        for url in self.start_urls:
            yield scrapy.Request(url, dont_filter=True, headers=self.headers)
