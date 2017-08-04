from isadb.conditions import BaseCondition


class BaseJoin:
    def __init__(self, symbol, conditions: list):
        self.conditions = []
        self.symbol = " {symbol} ".format(symbol=symbol.strip())
        self.is_join = False

        if not isinstance(conditions, list):
            conditions = [conditions]

        if len(conditions) == 1 and isinstance(conditions[0], BaseJoin) or isinstance(conditions[0], BaseCondition):
            self.is_join = True
            self.conditions = conditions[0].format()
        else:
            for condition in conditions:
                if isinstance(condition, BaseCondition) or isinstance(condition, BaseJoin):
                    self.conditions.append(condition.format())
                else:
                    raise TypeError('must a condition class')

    def format(self):
        if self.is_join:
            return self.conditions
        return "(%s)" % self.symbol.join(self.conditions)


class AndJoin(BaseJoin):
    def __init__(self, conditions):
        super().__init__(symbol='AND', conditions=conditions)


class OrJoin(BaseJoin):
    def __init__(self, conditions):
        super().__init__(symbol='OR', conditions=conditions)
