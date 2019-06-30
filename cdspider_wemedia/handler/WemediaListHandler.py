#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-11 11:10:18
"""
import copy
import time
import traceback
from cdspider.handler import GeneralHandler
from urllib.parse import urljoin
from cdspider.database.base import *
from cdspider_wemedia.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser
from cdspider.parser.lib import TimeParser

class WemediaListHandler(GeneralHandler):
    """
    wemedia list handler
    :property task 爬虫任务信息 {"mode": "wemedia-list", "uuid": SpiderTask.author uuid}
                   当测试该handler，数据应为 {"mode": "wemedia-list", "author": 自媒体号设置，参考自媒体号, "authorListRule": 自媒体列表规则，参考自媒体列表规则}
    """

    def new_author_task(self, urls, author):
        kfrequency = author.get("frequency")
        frequency = urls.get("frequency", self.DEFAULT_FREQUENCY)
        if kfrequency and kfrequency < frequency:
            frequency = kfrequency
        kexpire = author.get("expire", 0)
        expire = urls.get("expire", 0)
        if kexpire != 0 and expire != 0 and kexpire < expire:
            expire = kexpire
        elif expire == 0:
            expire = kexpire or 0
        t = {
            'mode': self.mode,     # handler mode
            'pid': urls['pid'],          # project uuid
            'sid': urls['sid'],          # site uuid
            'tid': urls['tid'],          # task uuid
            'uid': urls['uuid'],         # url uuid
            'kid': author['uuid'],      # author id
            'url': 'base_url',           # url
            'frequency': str(frequency),
            'status': SpiderTaskDB.STATUS_ACTIVE,
            'expire': int(time.time()) + int(expire) if int(expire) > 0 else 0
        }
        self.debug("%s newtask new task: %s" % (self.__class__.__name__, str(t)))
        if not self.testing_mode:
            '''
            testing_mode打开时，数据不入库
            '''
            self.db['SpiderTaskDB'].insert(t)

    def new_author_task_by_tid(self, tid, author):
        uuid = 0
        while True:
            has_item = False
            for urls in self.db['UrlsDB'].get_new_list(uuid, where={"tid": tid}):
                tasks = self.db['SpiderTaskDB'].get_list(self.mode, {"tid": tid, "kid": author['uuid']})
                has_item = True
                uuid = urls['uuid']
                if len(list(tasks)) > 0:
                    continue
                self.new_search_task(urls, author)
            if has_item is False:
                return

    def newtask(self, message):
        """
        新建爬虫任务
        :param message [{"uid": url uuid, "kid": author uuid, "mode": handler mode}]
        """
        self.debug("%s newtask got message: %s" % (self.__class__.__name__, str(message)))
        if 'uid' in message and message['uid']:
            uid = message['uid']
            if not isinstance(uid, (list, tuple)):
                uid = [uid]
            for each in uid:
                tasks = self.db['SpiderTaskDB'].get_list(message['mode'], {"uid": each})
                if len(list(tasks)) > 0:
                    continue
                urls = self.db['UrlsDB'].get_detail(each)
                if not urls:
                    raise CDSpiderDBDataNotFound("urls: %s not found" % each)
                uuid = 0
                while True:
                    has_word = False
                    for item in self.db['AuthorDB'].get_new_list(uuid, urls['tid'], select=['uuid']):
                        self.new_author_task(urls, item)
                        uuid = item['uuid']
                        has_word = True
                    if not has_word:
                        break
        else:
            kid = message['kid']
            if not isinstance(kid, (list, tuple)):
                kid = [kid]
            for each in kid:
                author = self.db['AuthorDB'].get_detail(each)
                if not author:
                    raise CDSpiderDBDataNotFound("author: %s not found" % each)
                if "tid" in author and author['tid'] != 0:
                    task = self.db['TaskDB'].get_detail(author['tid'])
                    if not task:
                        raise CDSpiderDBDataNotFound("task: %s not found" % author['tid'])
                    self.new_search_task_by_tid(task['uuid'], author)

    def match_rule(self, save):
        """
        获取匹配的规则
        """
        if "rule" in self.task and self.task['rule']:
            '''
            如果task中包含列表规则，则读取相应的规则，否则在数据库中查询
            '''
            rule = self.db['ListRuleDB'].get_detail(int(self.task['rule']))
            if not rule:
                raise CDSpiderDBDataNotFound("rule: %s not exists" % self.task['rule'])

            author = self.db['AuthorDB'].get_detail(self.task['author'])
            if not author:
                raise CDSpiderDBDataNotFound("author: %s not exists" % self.task['author'])
        else:
            author = self.db['AuthorDB'].get_detail(self.task['kid'])
            if not author:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.mode)
                raise CDSpiderDBDataNotFound("author: %s not exists" % self.task['kid'])
            if author['status'] != AuthorDB.STATUS_ACTIVE:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.mode)
                raise CDSpiderHandlerError("author: %s not active" % self.task['uid'])
            urls = self.db['UrlsDB'].get_detail(self.task['uid'])
            if not urls:
                self.db['SpiderTaskDB'].delete(self.task['uuid'], self.mode)
                raise CDSpiderDBDataNotFound("urls: %s not exists" % self.task['uid'])
            if urls['status'] != UrlsDB.STATUS_ACTIVE or urls['ruleStatus'] != UrlsDB.STATUS_ACTIVE:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.mode)
                raise CDSpiderHandlerError("url not active")
            rule = self.db['ListRuleDB'].get_detail(urls['ruleId'])
            if not rule:
                self.db['SpiderTaskDB'].disable(self.task['uuid'], self.mode)
                raise CDSpiderDBDataNotFound("task rule by tid: %s not exists" % self.task['tid'])
            if rule['status'] != ListRuleDB.STATUS_ACTIVE:
                raise CDSpiderHandlerError("author rule: %s not active" % rule['uuid'])
        parameters = author.get('parameters')
        if parameters:
            save['request'] = parameters
        self.task['url'] = rule['baseUrl']
        save['base_url'] = rule['baseUrl']
        return rule

