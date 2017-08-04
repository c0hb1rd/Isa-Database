# 数据库对象
from isadb.core.table import Table
from isadb.libs import SerializedInterface


class Database(SerializedInterface):
    def __init__(self, name):
        self.__name = name  # 数据库名字
        self.__table_names = []  # 所有数据表名
        self.__table_objs = {}  # 数据库表名与表对象映射

    # 获取数据表名字
    def get_table(self, index=None):
        length = len(self.__table_names)

        if isinstance(index, int) and -index < length > index:
            return self.__table_names[index]
        return self.__table_names

    # 获取数据表对象
    def get_table_obj(self, name):
        return self.__table_objs.get(name, None)

    # 获取数据表对象所有字段
    def get_table_fields(self, name):
        return self.get_table_obj(name).get_fields()

    def drop_tables(self, table_name):
        if table_name not in self.__table_names:
            raise Exception('%s table not exist' % table_name)
        self.__table_names.remove(table_name)
        self.__table_objs.pop(table_name, True)

    def create_table(self, table_name, **options):
        if table_name in self.__table_objs:
            raise Exception('table always exists')
        self.__table_names.append(table_name)
        self.__table_objs[table_name] = Table(**options)

    def add_table(self, table_name, table):
        if table_name not in self.__table_objs:
            self.__table_names.append(table_name)
            self.__table_objs[table_name] = table

    def get_name(self):
        return self.__name

    @staticmethod
    def deserialized(obj):
        data = SerializedInterface.json.loads(obj)
        obj_tmp = Database(data['name'])
        for table_name, table_obj in data['tables']:
            obj_tmp.add_table(table_name, Table.deserialized(table_obj))

        return obj_tmp

    def serialized(self):
        data = {'name': self.__name, 'tables': []}

        for tb_name, tb_data in self.__table_objs.items():
            data['tables'].append(
                [tb_name, tb_data.serialized()]
            )

        return SerializedInterface.json.dumps(data)
