class BaseCondition:
    def __init__(self, field, case, symbol):
        self.field = field
        self.case = case
        self.symbol = symbol

    def format(self):
        case = self.case
        if isinstance(self.case, str):
            case = '"{case}"'.format(case=case)

        return '`{field}` {symbol} {case}'.format(field=self.field, symbol=self.symbol, case=case)


class IsCondition(BaseCondition):
    def __init__(self, field, case, flag: bool=True):
        if flag:
            symbol = '='
        else:
            symbol = '!='
        super().__init__(field, case, symbol=symbol)


class IsGreaterCondition(BaseCondition):

    def __init__(self, field, case, flag: bool=True):
        if flag:
            symbol = '>'
        else:
            symbol = '<'
        super().__init__(field, case, symbol=symbol)


class LikeCondition(BaseCondition):
    def __init__(self, field, case):
        super().__init__(field, case, symbol='like')


class IsInCondition(BaseCondition):
    def __init__(self, field, case, flag: bool=True):
        if not isinstance(case, list):
            raise TypeError('must a list')
        values = []

        for value in case:
            if isinstance(value, int):
                values.append(str(value))
            elif isinstance(value, str):
                values.append('"{value}"'.format(value=value))

        case = '({values})'.format(values=", ".join(values))
        if flag:
            symbol = 'IN'
        else:
            symbol = 'NOT IN'
        super().__init__(field, case, symbol=symbol)

    def format(self):
        return '`{field}` {symbol} {case}'.format(field=self.field, symbol=self.symbol, case=self.case)
