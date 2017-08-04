import base64
import os

from prettytable import PrettyTable

from isadb.core.database import Database
from isadb.libs import SerializedInterface
from isadb.parser import parse


def _decode_db(content):
    content = base64.decodebytes(content)
    return content.decode()[::-1]


def _encode_db(content):
    content = content[::-1].encode()
    return base64.encodebytes(content)


# 数据库引擎
class Engine:
    # 数据库映射表
    __database_objs = {}

    def __init__(self, db_name=None, format_type='dict', path='db.dat'):
        self.path = path
        self.__database_names = []
        self.__database_objs = {}
        self.__current_db = None
        self.__format_type = format_type
        self.__load_databases()

        if db_name:
            self.select_db(db_name)

    # 加载数据库
    def __load_databases(self):
        if not os.path.exists(self.path):
            return
        with open(self.path, 'rb') as f:
            content = f.read()

        content = _decode_db(content)
        self.deserialized(content)

    # 保存数据库
    def __dump_databases(self):
        with open(self.path, 'wb') as f:
            content = _encode_db(self.__serialized())
            f.write(content)

    # 获取数据库
    def get_database(self, index=None, format_type='list'):
        length = len(self.__database_names)
        if isinstance(index, int) and -length < index > length:
            databases = self.__database_names[index]
        else:
            databases = self.__database_names

        if format_type == 'dict':
            tmp = []
            for database in databases:
                tmp.append({'name': database})

            databases = tmp

        return databases

    # 获取数据表
    def get_table(self, format_type='list'):
        self.__check_is_choose()

        tables = self.__current_db.get_table()

        if format_type == 'dict':
            tmp = []
            for table in tables:
                tmp.append({'name': table})

            tables = tmp

        return tables

    # 删除数据库
    def drop_database(self, database_name):
        if database_name not in self.__database_objs:
            raise Exception('%s database not exists' % database_name)
        self.__database_names.remove(database_name)
        self.__database_objs.pop(database_name, True)

    # 删除数据库
    def create_database(self, database_name):
        if database_name in self.__database_objs:
            raise Exception('Database %s Exist' % database_name)
        _database = Database(database_name)
        self.__database_names.append(database_name)
        self.__database_objs[database_name] = _database

    # 删除数据表
    def drop_table(self, table_name):
        self.__check_is_choose()
        self.__current_db.drop_tables(table_name)

    # 提交数据库改动
    def commit(self):
        self.__dump_databases()

    # 回滚数据库到改动之前
    def rollback(self):
        self.__load_databases()

    # 选择数据库
    def select_db(self, db_name):
        # 如果不存在该数据库索引，抛出数据库不存在异常
        if db_name not in self.__database_objs:
            raise Exception('has not this database')
        self.__current_db = self.__database_objs[db_name]

    #
    def create_table(self, name, **options):
        self.__check_is_choose()
        self.__current_db.create_table(name, **options)

    # 执行动作
    def execute(self, statement):
        action = parse(statement)

        if action['type'] == 'insert':
            table = action['table']
            data = action['data']
            ret = self.insert(table, data=data)

        if action['type'] == 'show':
            if action['kind'] == 'databases':
                ret = self.get_database(format_type='dict')
            else:
                ret = self.get_table(format_type='dict')

        if action['type'] == 'drop':
            if action['kind'] == 'database':
                ret = self.drop_database(action['name'])
            else:
                ret = self.drop_table(action['name'])

        if action['type'] == 'exit':
            return 'exit'

        if action['type'] == 'update':
            table = action['table']
            data = action['data']
            conditions = action['conditions']
            ret = self.update(table, data, conditions=conditions)

        if action['type'] == 'delete':
            table = action['table']
            conditions = action['conditions']
            ret = self.delete(table, conditions=conditions)

        if action['type'] == 'search':
            table = action['table']
            fields = action['fields']
            conditions = action['conditions']

            ret = self.search(table, fields=fields, conditions=conditions)
        if action['type'] == 'use':
            ret = self.select_db(action['database'])
        return ret

    # 插入数据到指定数据表
    def insert(self, table_name, **data):
        return self.__get_table(table_name).insert(**data)

    # 查询指定数据表数据
    def search(self, table_name, fields='*', sort='ASC', **conditions):
        return self.__get_table(table_name).search(fields=fields, sort=sort, format_type=self.__format_type,
                                                   **conditions)

    # 更新指定数据表数据
    def update(self, table_name, data, **conditions):
        self.__get_table(table_name).update(data, **conditions)

    # 删除指定数据表数据
    def delete(self, table_name, **conditions):
        return self.__get_table(table_name).delete(**conditions)

    # 获取指定数据表数据长度
    def get_table_length(self, table_name):
        return self.__get_table(table_name).length()

    # 获取数据表
    def __get_table(self, table_name):
        self.__check_is_choose()

        table = self.__current_db.get_table_obj(table_name)

        if table is None:
            raise Exception('not table %s' % table_name)
        return table

    # 序列化数据库
    def deserialized(self, content):
        data = SerializedInterface.json.loads(content)

        for obj in data:
            database = Database.deserialized(obj)
            db_name = database.get_name()
            self.__database_names.append(db_name)
            self.__database_objs[db_name] = database

    # 反序列化数据库
    def __serialized(self):
        return SerializedInterface.json.dumps([
            database.serialized() for database in self.__database_objs.values()
        ])

    # 检查是否选择数据库
    def __check_is_choose(self):
        if not self.__current_db or not isinstance(self.__current_db, Database):
            raise Exception('No database choose')

    # 交互
    def run(self):
        while True:
            statement = input('\033[00;37misadb> ')
            try:
                ret = self.execute(statement)
                if ret in ['exit', 'quit']:
                    print('Goodbye!')
                    return
                if ret:
                    pt = PrettyTable(ret[0].keys())
                    for line in ret:
                        pt.add_row(line.values())
                    print(pt)
            except Exception as exc:
                print('\033[00;31m' + str(exc))
