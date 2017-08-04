import json


# 数据库成员公有接口
class SerializedInterface:
    json = json

    @staticmethod
    def deserialized(obj):
        raise NotImplementedError

    def serialized(self):
        raise NotImplementedError
