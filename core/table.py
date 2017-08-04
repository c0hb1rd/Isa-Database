from isadb.case import BaseCase
from isadb.core.field import Field
from isadb.libs import SerializedInterface


# 数据表对象
class Table(SerializedInterface):
    def __init__(self, **options):
        self.__field_names = []  # 数据表所有字段名
        self.__field_objs = {}  # 数据表字段名与字段对象映射
        self.__rows = 0

        # 获取所有字段名和字段对象为数据表初始化字段
        for field_name, field_obj in options.items():
            # 进行字段添加
            self.add_field(field_name, field_obj)

    # 添加新字段
    def add_field(self, field_name, field_obj, value=None):

        # 如果新添加的字段名已存在，抛出字段已存在异常
        if field_name in self.__field_names:
            raise Exception('Field Exists')

        # 如果 field_obj 不为 Field 对象，抛出类型错误异常
        if not isinstance(field_obj, Field):
            raise TypeError('type error, value must be %s' % Field)

        # 添加字段名
        self.__field_names.append(field_name)

        # 绑定字段名与字段
        self.__field_objs[field_name] = field_obj

        # 如果已存在其它字段，同步该新增字段的数据长度与原先字段数据长度等长，反之初始化数据长度为第一个字段的数据长度
        if len(self.__field_names) > 1:

            # 获取已存在字段的长度
            length = self.__rows

            # 获取该新增字段的长度
            field_obj_length = field_obj.length()

            # 如果该新增字段自身包含数据，则判断长度是否与已存在字段的长度相等
            if field_obj_length != 0:
                # 相等，退出函数
                if field_obj_length == length:
                    return

                # 不相等，抛出数据长度异常
                raise Exception('Field data length inconformity')

            # 循环初始化新增字段数据，直到新增字段的数据长度到与已存在字段的数据长度相等
            for index in range(0, length):
                # 如果指定了初始值，则使用初始值
                if value:
                    self.__get_field(field_name).add(value)
                # 反之，用空值
                else:
                    self.__get_field(field_name).add(None)
        else:
            # 初始化表格所有数据长度为第一个字段的数据长度
            self.__rows = field_obj.length()

    # 获取数据长度
    def length(self):
        return self.__rows

    # 获取字段名
    def get_field(self, index=None):
        length = len(self.__field_names)
        if isinstance(index, int) and -index < length > index:
            return self.__field_names[index]
        return self.__field_names

    # 查询数据
    def search(self, fields, sort, format_type, **conditions):

        if fields == '*':
            fields = self.__field_names
        else:
            for field in fields:
                if field not in self.__field_names:
                    raise Exception('%s field not exists' % field)

        # 初始化查询结果变量为一个空的 list
        rows = []

        # 解析条件，并返回符合条件的数据索引
        match_index = self.__parse_conditions(**conditions)

        # 遍历符合条件的数据索引，根据指定的返回格式返回数据
        for index in match_index:
            # 返回 list 类型的数据，也就是没有字段名
            if format_type == 'list':
                row = [self.__get_field_data(field_name, index) for field_name in fields]
            # 返回 dict 类型的数据，也就是字段和值成健值对
            elif format_type == 'dict':
                row = {}
                for field_name in fields:
                    row[field_name] = self.__get_field_data(field_name, index)
            # 如果找不到类型，抛出格式错误异常
            else:
                raise Exception('format type invalid')
            rows.append(row)

        # 如排序方式为倒序，则倒序结果
        if sort == 'DESC':
            rows = rows[::-1]

        # 返回查询结果
        return rows

    # 插入数据
    def insert(self, **data):
        # 解析参数

        if 'data' in data:
            data = data['data']

        name_tmp = self.__get_name_tmp(**data)

        # 遍历插入数据字段
        for field_name in self.__field_names:

            value = None

            # 如果存在该字段，则添加对应的值
            if field_name in name_tmp:
                value = data[field_name]

            # 如果不存在，则添加一个空值保持该字段长度与其它字段长度相等
            try:
                self.__get_field(field_name).add(value)
            except Exception as e:
                raise Exception(field_name, str(e))

        # 数据长度加一
        self.__rows += 1

    # 更新数据
    def update(self, data, **conditions):

        # 解析条件，并返回符合条件的数据索引
        match_index = self.__parse_conditions(**conditions)

        name_tmp = self.__get_name_tmp(**data)

        for field_name in name_tmp:
            for index in match_index:
                self.__get_field(field_name).modify(index, data[field_name])

    def delete(self, **conditions):

        # 解析条件，并返回符合条件的数据索引
        match_index = self.__parse_conditions(**conditions)

        for field_name in self.__field_names:
            count = 0
            max_tmp = 0
            for index in match_index:
                if index > max_tmp:
                    max_tmp = index
                    index = index - count
                self.__get_field(field_name).delete(index)
                count += 1

        self.__rows = self.__get_field_length(self.__field_names[0])

    # 反序列化对象
    @staticmethod
    def deserialized(data):
        # 将数据转化为 Json 对象
        json_data = SerializedInterface.json.loads(data)

        # 实例化一个 Table 对象
        table_obj = Table()

        # 获取所有字段名
        field_names = [field_name for field_name in json_data.keys()]

        # 遍历所有字段对象
        for field_name in field_names:
            # 反序列化 Field 对象
            field_obj = Field.deserialized(json_data[field_name])

            # 将 Field 对象添加到 Table 对象中
            table_obj.add_field(field_name, field_obj)

        # 返回 Table 对象
        return table_obj

    # 序列化对象
    def serialized(self):
        data = {}
        for field in self.__field_names:
            data[field] = self.__field_objs[field].serialized()

        return SerializedInterface.json.dumps(data)

    # 解析条件
    def __parse_conditions(self, **conditions):

        if 'conditions' in conditions:
            conditions = conditions['conditions']

        # 如果条件为空，数据索引为所有，反之为匹配条件的索引
        if not conditions:
            match_index = range(0, self.__rows)
        else:
            # 解析条件，并返回符合条件的数据索引
            name_tmp = self.__get_name_tmp(**conditions)

            # 存放匹配上一次条件的索引
            match_tmp = []

            # 存放匹配所有条件的索引
            match_index = []

            # 定一个以标示，用于判断是否是第一次循环
            is_first = True

            # 遍历所有参数字段名
            for field_name in name_tmp:
                # 获取字段所有数据
                data = self.__get_field_data(field_name)

                # 获取字段类型
                data_type = self.__get_field_type(field_name)

                # 获取对应的判断条件
                case = conditions[field_name]

                # 如果 case 变量不属于 BaseCase 类，抛出类型错误异常
                if not isinstance(case, BaseCase):
                    raise TypeError('Type error, value must be "Case" object')

                # 如果为第一次循环
                if is_first:
                    # 获取字段长度
                    length = self.__get_field(field_name).length()

                    # 遍历所有数据的索引
                    for index in range(0, length):
                        # 如果判断条件成立，则将数据对应的索引追加到两个匹配索引 list 中
                        if case(data[index], data_type):
                            match_tmp.append(index)
                            match_index.append(index)

                    # 使判断标识失效
                    is_first = False

                    # 重新进入循环
                    continue

                # 如果不是第一次循环，遍历上一次匹配成功的索引
                for index in match_tmp:
                    # 如果判断条件不成立，则从匹配所有条件的 list 中删除对应索引
                    if not case(data[index], data_type):
                        match_index.remove(index)

                # 同步匹配结果到 match_tmp 中
                match_tmp = match_index

        # 返回所有匹配的索引
        return match_index

    # 解析参数中包含的字段名
    def __get_name_tmp(self, **options):
        # 初始化参数名存放 list
        name_tmp = []

        conditions = options

        # 获取所有参数名字，也就是字段名字
        if 'conditions' in options:
            conditions = options['conditions']

        for field_name in conditions.keys():
            # 如果参数名字不在字段名字列表中，抛出字段不存在异常
            if field_name not in self.__field_names:
                raise Exception('%s Field Not Exists' % field_name)

            # 如果存在，追加到 name_tmp 这个 list 中
            name_tmp.append(field_name)

        # 返回所有参数名
        return name_tmp

    # 获取 Field 对象
    def __get_field(self, field_name):
        if field_name not in self.__field_names:
            raise Exception('%s field not exists' % field_name)
        return self.__field_objs[field_name]

    # 获取字段中的数据
    def __get_field_data(self, field_name, index=None):
        # 获取 Field 对象
        field = self.__get_field(field_name)

        # 调用 Field 对象的 get_data 方法并返回其获取到的结果
        return field.get_data(index)

    def __get_field_type(self, field_name):
        # 获取 Field 对象
        field = self.__get_field(field_name)

        # 调用 Field 对象的 get_type 方法并返回其获取到的结果
        return field.get_type()

    def __get_field_length(self, field_name):
        # 获取 Field 对象
        field = self.__get_field(field_name)

        # 调用 Field 对象的 get_length 方法并返回其获取到的结果
        return field.length()
