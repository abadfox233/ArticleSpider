# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import datetime
from w3lib.html import remove_tags
from ArticleSpider.utils.common import extract_num
from ArticleSpider.settings import SQL_DATE_FORMAT, SQL_DATETIME_FORMAT
from scrapy.loader import ItemLoader

from ArticleSpider.models.es_jobbole import ArticleType
from ArticleSpider.models.es_zhihu import ZhiHuAnswerType, ZhiHuQuestionType
from ArticleSpider.models.es_lagou import LagouType

from scrapy.loader.processors import MapCompose, TakeFirst, Join

from elasticsearch_dsl.connections import connections
es = connections.create_connection(ArticleType._doc_type.using)


def gen_suggests(index, info_tuple):
    # 根据字符串生成搜索建议数组
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es的analyze接口分析字符串
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter': ["lowercase"]}, body=text)
            anylyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"]) > 1])
            new_words = anylyzed_words - used_words

        else:
            new_words = set()

        if new_words:
            suggests.append({"input": list(new_words), "weight": weight})
            used_words |= new_words

    return suggests


class ArticlespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class JobBoleArticleItem(scrapy.Item):
    # 伯乐在线
    title = scrapy.Field()
    create_date = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field()
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field()
    comment_nums = scrapy.Field()
    fav_nums = scrapy.Field()
    tags = scrapy.Field()
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = "insert into jobbole_article(title, create_date, url, " \
                     "url_object_id, front_image_url, front_image_path, " \
                     "comment_nums, fav_nums, praise_nums, tags, content) " \
                     "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)ON DUPLICATE KEY UPDATE content=VALUES(fav_nums)"

        params = (
                    self['title'], self['create_date'], self['url'], self['url_object_id'],
                    self['front_image_url'] if self['front_image_url'] else 'NULL',
                    self['front_image_path'] if self['front_image_path'] else 'NULL',
                    self['comment_nums'], self['fav_nums'], self['praise_nums'], self['tags'],
                    self['content']
                )

        return insert_sql, params

    def save_to_es(self):
        # 保存到es
        article = ArticleType()
        article.title = self['title']
        article.create_date = self["create_date"]
        if isinstance(self['content'], list):
            self['content'] = ''.join(self['content'])
        self["content"].strip().replace("\r\n", "").replace("\t", "")
        article.content = remove_tags(self["content"])
        article.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            article.front_image_path = self["front_image_path"]
        article.praise_nums = self["praise_nums"]
        article.fav_nums = self["fav_nums"]
        article.comment_nums = self["comment_nums"]
        article.url = self["url"]
        article.tags = self["tags"]
        article.meta.id = self["url_object_id"]

        article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7)))

        article.save()

        # redis_cli.incr("jobbole_count")

        return


class ZhihuQuesionItem(scrapy.Item):
    # 知乎问题
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                    insert into zhihu_question(
                    zhihu_id, topics, url, title, content, answer_num, comments_num,
                      watch_user_num, click_num, crawl_time
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE content=VALUES(content), answer_num=VALUES(answer_num), comments_num=VALUES(comments_num),
                      watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
                """

        zhihu_id = self["zhihu_id"][0]
        topics = ",".join(self["topics"])
        url = self["url"][0]
        title = "".join(self["title"])
        content = "".join(self["content"])
        try:
            answer_num = extract_num(("".join(self["answer_num"])).replace(',', ''))
        except:
            answer_num = 0
        comments_num = extract_num(("".join(self["comments_num"])).replace(',', ''))
        if len(self["watch_user_num"]) == 2:
            watch_user_num = int((self["watch_user_num"][0]).replace(',', ''))
            click_num = int(self["watch_user_num"][1].replace(',', ''))
        else:
            watch_user_num = int((self["watch_user_num"][0]).replace(',', ''))
            click_num = 0

        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (zhihu_id, topics, url, title, content, answer_num, comments_num,
                  watch_user_num, click_num, crawl_time)

        return insert_sql, params

    def save_to_es(self):
        question = ZhiHuQuestionType()
        question.zhihu_id = self['zhihu_id']
        question.topics = self['topics']
        question.url = self['url']
        question.title = self['title']
        if isinstance(self['content'], list):
            self['content'] = ''.join(self['content'])
        self["content"].strip().replace("\r\n", "").replace("\t", "")
        question.content = self['content']
        try:
            answer_num = extract_num(("".join(self["answer_num"])).replace(',', ''))
        except:
            answer_num = 0
        comments_num = extract_num(("".join(self["comments_num"])).replace(',', ''))
        if len(self["watch_user_num"]) == 2:
            watch_user_num = int((self["watch_user_num"][0]).replace(',', ''))
            click_num = int(self["watch_user_num"][1].replace(',', ''))
        else:
            watch_user_num = int((self["watch_user_num"][0]).replace(',', ''))
            click_num = 0
        question.answer_num = answer_num
        question.comments_num = comments_num
        question.watch_user_num = watch_user_num
        question.click_num = click_num
        question.crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        question.suggest = gen_suggests(ZhiHuQuestionType._doc_type.index, ((question.title, 10), (question.topics, 7)))
        question.save()
        return


class ZhihuAnswerItem(scrapy.Item):
    # 知乎回答
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num, comments_num,
              create_time, update_time, crawl_time
              ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              ON DUPLICATE KEY UPDATE content=VALUES(content), comments_num=VALUES(comments_num), praise_num=VALUES(praise_num),
              update_time=VALUES(update_time)
        """

        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        params = (
            self["zhihu_id"], self["url"], self["question_id"],
            self["author_id"], self["content"], self["praise_num"],
            self["comments_num"], create_time, update_time,
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params

    def save_to_es(self):
        answer = ZhiHuAnswerType()
        answer.zhihu_id = self['zhihu_id']
        answer.url = self['url']
        if isinstance(self['content'], list):
            self['content'] = ''.join(self['content'])
        self["content"].strip().replace("\r\n", "").replace("\t", "")
        answer.content = self['content']
        answer.question_id = self['question_id']
        answer.author_id = self['author_id']
        answer.praise_num = self['praise_num']
        answer.comments_num = self['comments_num']
        answer.create_time = self['create_time']
        answer.update_time = self['update_time']
        answer.crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        # article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7)))
        answer.save()
        return


def remove_splash(value):
    # 拉钩 去斜杠
    return value.replace('/', '')


class LagouJobItemLoader(ItemLoader):
    #自定义 拉钩 itemloader
    default_output_processor = TakeFirst()


def handle_jobaddr(value):
    # 拉钩 处理 地址
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)


class LagouJobItem(scrapy.Item):

    #拉勾网职位信息

    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr),
    )
    company_name = scrapy.Field()
    company_url = scrapy.Field()
    tags = scrapy.Field(
        input_processor=Join(",")
    )
    crawl_time = scrapy.Field()

    def get_insert_sql(self):

        insert_sql = """
            insert into lagou_job(title, url, url_object_id, salary, job_city, work_years, degree_need,
            job_type, publish_time, job_advantage, job_desc, job_addr, company_name, company_url,
            tags, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """

        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["job_city"],
            self["work_years"], self["degree_need"], self["job_type"],
            self["publish_time"], self["job_advantage"], self["job_desc"],
            self["job_addr"], self["company_name"], self["company_url"],
            self["job_addr"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params

    def save_to_es(self):
        lagou = LagouType()
        lagou.title = self['title']
        lagou.url = self['url']
        lagou.url_object_id = self['url_object_id']
        lagou.salary = self['salary']
        lagou.job_city = self['job_city']
        lagou.work_years = self['work_years']
        if isinstance(self['job_desc'], list):
            self['job_desc'] = ''.join(self['job_desc'])
        self["job_desc"].strip().replace("\r\n", "").replace("\t", "")
        lagou.job_desc = self["job_desc"]
        lagou.degree_need = self['degree_need']
        lagou.job_type = self['job_type']
        lagou.job_addr = self['job_addr']
        lagou.job_advantage = self['job_advantage']
        lagou.publish_time = self['publish_time']
        lagou.company_name = self['company_name']
        lagou.company_url = self['company_url']
        if "tags" in self:
            lagou.tags = self["tags"]
        lagou.crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        # article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7)))
        lagou.suggest = gen_suggests(LagouType._doc_type.index, ((lagou.title, 10), (lagou.company_name, 4),
                                                                 (lagou.job_city, 2)))
        lagou.save()
        return
