class DBException(Exception):
    pass


class DatabaseNotExistsError(DBException):
    pass


class TableNotExistsError(DBException):
    pass


class FieldNotExistsError(DBException):
    pass


class DatabaseTypeError(DBException):
    pass


class TableTypeError(DBException):
    pass


class FieldTypeError(DBException):
    pass


class CaseTypeError(DBException):
    pass
