# -*- coding: utf-8 -*-
import codecs
from scrapy.pipelines.images import ImagesPipeline
import json
from scrapy.exporters import JsonItemExporter
import MySQLdb
from twisted.enterprise import adbapi
import MySQLdb.cursors
from w3lib.html import remove_tags
from ArticleSpider.models.es_jobbole import ArticleType
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item


class JsonExporterPipeline(object):
    '''调用 scrapy提供的json exporter 到处json文件'''
    def __init__(self):
        self.file = open('articleExport.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf8', ensure_ascii=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()


class ArticleImagePipeline(ImagesPipeline):
    'jobblog 图片'
    '''
    下载图片
    '''
    def item_completed(self, results, item, info):
        for ok, value in results:
            image_file_path = value["path"]
        item["front_image_path"] = image_file_path
        return item


class MysqlTwistedPipeline(object):

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True
             )
        dppoll = adbapi.ConnectionPool("MySQLdb", **dbparms)
        return cls(dppoll)

    def __init__(self, dbpool):
        self.dbpool = dbpool

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item)
        return item

    def handle_error(self, failure, item):
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)


class JsonWithEncodingPipeline(object):
    '''
    自定义 保存为json文本
    '''
    def __init__(self):
        self.file = codecs.open("article.json", 'w', encoding='utf8')

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        return item

    def spider_closed(self, spider):
        self.file.close()


class MysqlPipeline(object):
    '''
    自定义保存到数据库
    '''
    def __init__(self):
        self.conn = MySQLdb.connect("localhost", 'root', '7411', 'article_spider',
                                    charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = "insert into jobbole_article(title, create_date, url, url_object_id, front_image_url, front_image_path, comment_nums, fav_nums, praise_nums, tags, content) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        self.cursor.execute(insert_sql, (item['title'],item['create_date'],item['url'],
                                         item['url_object_id'],item['front_image_url'],item['front_image_path'],
                                         item['comment_nums'],item['fav_nums'],item['praise_nums'],item['tags'],item['content']
                                             ))
        self.conn.commit()
        return item


class ElasticsearchPipeline(object):

    # 将数据写入es
    def process_item(self, item, spider):
        item.save_to_es()

        return item
