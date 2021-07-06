#!/usr/bin/python
# -*- coding: utf-8 -*-

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import pymysql
import logging
from datetime import datetime
from dingtalkchatbot import chatbot


class dbMongo(object):

    def __init__(self, name=None, password=None, ip="localhost", port=27017, dbName="admin"):
        self.connect(name, password, ip, port, dbName)

    # ----------------------------------------------------------------------
    def connect(self, name, password, ip, port, dbName):
        """连接MongoDB数据库"""

        # 读取MongoDB的设置
        try:
            MONGOHOST = "localhost"
            MONGOPORT = 27017
            # 设置MongoDB操作的超时时间为0.5秒
            if not name:
                self.dbClient = MongoClient(MONGOHOST, MONGOPORT, connect=False)
            else:
                self.dbClient = MongoClient("mongodb://{0}:{1}@{2}:{3}/{4}".format(name, password, ip, port, dbName), connect=False)
            # 调用server_info查询服务器状态，防止服务器异常并未连接成功
            self.dbClient.server_info()

        except ConnectionFailure:
            print('连接失败')

    # ----------------------------------------------------------------------
    def dbInsert(self, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        """
        db = self.dbClient[dbName]
        collection = db[collectionName]
        collection.insert_one(d)
        """
        try:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.insert_one(d)
        except DuplicateKeyError:
            print('重复插入，插入失败，请使用更新: ' + str(d))


    # ----------------------------------------------------------------------
    def dbQuery(self, dbName, collectionName, d, sortKey='', sortDirection=ASCENDING):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        try:
            db = self.dbClient[dbName]
            collection = db[collectionName]

            if sortKey:
                cursor = collection.find(d).sort(sortKey, sortDirection)  # 对查询出来的数据进行排序
            else:
                # cursor = collection.find(d, projection={"_id": False})
                cursor = collection.find(d)

            if cursor:
                return list(cursor)
            else:
                return []
        except:
            print('查询失败')
            return []

    # ----------------------------------------------------------------------
    def dbQuery_ID(self, dbName, collectionName, d, sortKey='', sortDirection=ASCENDING):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        try:
            db = self.dbClient[dbName]
            collection = db[collectionName]

            if sortKey:
                #cursor = collection.find(d).sort(sortKey, sortDirection)  # 对查询出来的数据进行排序
                cursor = collection.find(d, projection={"_id": False}).sort(sortKey, sortDirection)
            else:
                cursor = collection.find(d, projection={"_id": False})
                #cursor = collection.find(d)

            if cursor:
                return list(cursor)
            else:
                return []
        except:
            print('查询失败')
            return []

    # ----------------------------------------------------------------------
    def dbUpdate(self, dbName, collectionName, d, flt, upsert=False):
        """向MongoDB中替换数据，d是具体数据，flt是过滤条件，upsert代表若无是否要插入"""
        try:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            # collection.ensure_index([('trade_date', 1)], unique=True)
            # collection.update_one(flt, d, upsert=True)
            collection.replace_one(flt, d, upsert)
        except Exception as e:
            print('更新失败: ' + str(e) + " " + str(d))

    # ----------------------------------------------------------------------
    def dbUpdate_one(self, dbName, collectionName, old_d, new_d, upsert=False):
        """向MongoDB中更新数据，d是具体数据，flt是过滤条件，upsert代表若无是否要插入"""
        try:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            # collection.ensure_index([('trade_date', 1)], unique=True)
            collection.update_one(old_d, new_d, upsert)
        except Exception as e:
            print('更新失败: ' + str(e) + " " + str(old_d))

    # ----------------------------------------------------------------------
    def dbDelete(self, dbName, collectionName, flt):
        """从数据库中删除数据，flt是过滤条件"""
        try:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.delete_one(flt)
        except:
            print('删除数据' + " " + str(flt))



class logtool(object):

    def __init__(self):
        pass

    def addHandler(self, name=None, path=None):
        logger = logging.getLogger()
        logger.setLevel(level=logging.INFO)
        
        if name:
            filename = name + '.log'
        else:
            filename = f"{datetime.now().strftime('%Y-%m-%d')}.log"
            
        if path:
            filename = path + filename
        else:
            filename = '/application/log/' + filename

        handler = logging.FileHandler(filename)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        logger.addHandler(handler)
        logger.addHandler(console)

        return logger



class dbMysql(object):
    '''python操作mysql的增删改查的封装'''

    def __init__(self, host, user, password, database, port=3306, charset='utf8'):
        '''
        初始化参数
        :param host:        主机
        :param user:        用户名
        :param password:    密码
        :param database:    数据库
        :param port:        端口号，默认是3306
        :param charset:     编码，默认是utf8
        '''
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.charset = charset

    def connect(self):
        '''
        获取连接对象和执行对象
        :return:
        '''
        self.conn = pymysql.connect(host=self.host,
                                    user=self.user,
                                    password=self.password,
                                    database=self.database,
                                    port=self.port,
                                    charset=self.charset)

        self.cur = self.conn.cursor()

    def fetchone(self, sql, params=None):
        '''
         根据sql和参数获取一行数据
       :param sql:          sql语句
       :param params:       sql语句对象的参数元组，默认值为None
       :return:             查询的一行数据
       '''
        dataOne = None
        try:
            count = self.cur.execute(sql, params)
            if count != 0:
                dataOne = self.cur.fetchone()
        except Exception as ex:
            print(ex)
        finally:
            self.close()
        return dataOne

    def fetchall(self, sql, params=None):
        '''
         根据sql和参数获取一行数据
       :param sql:          sql语句
       :param params:       sql语句对象的参数列表，默认值为None
       :return:             查询的一行数据
       '''
        dataall = None
        try:
            count = self.cur.execute(sql, params)
            if count != 0:
                dataall = self.cur.fetchall()
        except Exception as ex:
            print(ex)
        finally:
            self.close()
        return dataall

    def __item(self, sql, params=None):
        '''
        执行增删改
        :param sql:           sql语句
        :param params:        sql语句对象的参数列表，默认值为None
        :return:              受影响的行数
        '''
        count = 0
        try:
            count = self.cur.execute(sql, params)
            self.conn.commit()
        except Exception as ex:
            print(ex)
        finally:
            self.close()
        return count
        

    def update(self, sql, params=None):
        '''
        执行修改
        :param sql:     sql语句
        :param params:  sql语句对象的参数列表，默认值为None
        :return:        受影响的行数
        '''
        return self.__item(sql, params)

    def insert(self, sql, params=None):
        '''
        执行新增
        :param sql:     sql语句
        :param params:  sql语句对象的参数列表，默认值为None
        :return:        受影响的行数
        '''
        return self.__item(sql, params)

    def delete(self, sql, params=None):
        '''
        执行删除
        :param sql:     sql语句
        :param params:  sql语句对象的参数列表，默认值为None
        :return:        受影响的行数
        '''
        return self.__item(sql, params)

    def close(self):
        '''
        关闭执行工具和连接对象
        '''
        if self.cur != None:
            self.cur.close()
        if self.conn != None:
            self.conn.close()



class noticeDingDing(object):
    """dingding通知"""
    def __init__(self, webhook):
        self.ding = chatbot.DingtalkChatbot(webhook=webhook)

    def notice(self, msg, at_mobiles=[], keyword='info'):
        if at_mobiles:
            at_mobiles = at_mobiles
        try:
            msg_nomal = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + keyword + ": " + msg
            self.ding.send_text(msg_nomal, at_mobiles=at_mobiles)
        except Exception as e:
            msg_error = msg + " 出现问题 " + str(e)


class noticeTelMes(object):
    """twilio"""
    def __init__(self):
        pass

    def noticeMes(self, message):
        pass

    def noticeTel(self):
        pass

