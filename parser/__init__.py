import re

from isadb.case import *


def __select(statement):
    return {
        'type': 'search',
        'fields': statement[1],
        'table': statement[3].replace('`', '')
    }


def __update(statement):
    statement = ' '.join(statement)
    comp = __get_comp('UPDATE')
    ret = comp.findall(statement)
    if ret and len(ret[0]) == 2:
        data = {
            'type': 'update',
            'table': ret[0][0],
            'data': {}
        }
        set_statement = ret[0][1].split(",")
        for s in set_statement:
            s = s.split("=")
            field = s[0].strip()
            value = s[1].strip()
            if "'" in value or '"' in value:
                value = value.replace('"', '').replace("'", '').strip()
            else:
                try:
                    value = int(value.strip())
                except:
                    return None
            data['data'][field] = value

        return data
    return None


def __delete(statement):
    return {
        'type': 'delete',
        'table': statement[2]
    }


def __insert(statement):
    comp = __get_comp('INSERT')
    ret = comp.findall(" ".join(statement))

    if ret and len(ret[0]) == 3:
        ret_tmp = ret[0]
        data = {
            'type': 'insert',
            'table': ret_tmp[0],
            'data': {}
        }
        fields = ret_tmp[1].split(",")
        values = ret_tmp[2].split(",")

        for i in range(0, len(fields)):
            field = fields[i]
            value = values[i]
            if "'" in value or '"' in value:
                value = value.replace('"', '').replace("'", '').strip()
            else:
                try:
                    value = int(value.strip())
                except:
                    return None
            data['data'][field] = value
        return data
    return None


def __use(statement):
    return {
        'type': 'use',
        'database': statement[1]
    }


def __exit(_):
    return {
        'type': 'exit'
    }


def __show(statement):
    kind = statement[1]

    if kind.upper() == 'DATABASES':
        return {
            'type': 'show',
            'kind': 'databases'
        }
    if kind.upper() == 'TABLES':
        return {
            'type': 'show',
            'kind': 'tables'
        }


def __drop(statement):
    kind = statement[1]

    if kind.upper() == 'DATABASE':
        return {
            'type': 'drop',
            'kind': 'database',
            'name': statement[2]
        }
    if kind.upper() == 'TABLE':
        return {
            'type': 'drop',
            'kind': 'table',
            'name': statement[2]
        }


__pattern_map = {
    'SELECT': r'SELECT .* FROM .* WHERE (.*)]',
    'UPDATE': r'UPDATE (.*) SET (.*)',
    'DELETE': r'DELETE FROM (.*)',
    'INSERT': r'INSERT INTO (.*)\((.*)\) VALUES\((.*)\)'
}

__action_map = {
    'SELECT': __select,
    'UPDATE': __update,
    'DELETE': __delete,
    'INSERT': __insert,
    'USE': __use,
    'EXIT': __exit,
    'QUIT': __exit,
    'SHOW': __show,
    'DROP': __drop
}

SYMBOL_MAP = {
    'IN': InCase,
    'NOT_IN': NotInCase,
    '>': GreaterCase,
    '<': LessCase,
    '=': IsCase,
    '!=': IsNotCase,
    '>=': GAECase,
    '<=': LAECase,
    'LIKE': LikeCase,
    'RANGE': RangeCase
}


def __get_comp(action):
    return re.compile(__pattern_map[action])


def filter_space(obj):
    ret = []
    for x in obj:
        if x.strip() == '' or x.strip() == 'AND':
            continue
        ret.append(x)

    return ret


def parse(statement):
    tmp_s = statement
    if 'where' in statement:
        statement = statement.split("where")
    else:
        statement = statement.split("WHERE")

    base_statement = filter_space(statement[0].split(" "))

    if len(base_statement) < 2 and base_statement[0] not in ['exit', 'quit']:
        raise Exception('Syntax Error for: %s' % tmp_s)

    action_type = base_statement[0].upper()

    if action_type not in __action_map:
        raise Exception('Syntax Error for: %s' % tmp_s)

    action = __action_map[action_type](base_statement)

    if action is None or 'type' not in action:
        raise Exception('Syntax Error for: %s' % tmp_s)

    action['conditions'] = {}

    conditions = None

    if len(statement) == 2:
        conditions = filter_space(statement[1].split(" "))

    if conditions:
        for index in range(0, len(conditions), 3):
            field = conditions[index]
            symbol = conditions[index + 1].upper()
            condition = conditions[index + 2]

            if symbol == 'RANGE':
                condition_tmp = condition.replace("(", '').replace(")", '').split(",")
                start = condition_tmp[0]
                end = condition_tmp[1]
                case = SYMBOL_MAP[symbol](start, end)
            elif symbol == 'IN' or symbol == 'NOT_IN':
                condition_tmp = condition.replace("(", '').replace(")", '').replace(" ", '').split(",")
                condition = condition_tmp
                case = SYMBOL_MAP[symbol](condition)
            else:
                case = SYMBOL_MAP[symbol](condition)
            action['conditions'][field] = case
    return action

# statement = 'UPDATE f_iser SET f_id = 1, f_user = "c0hb1rd" WHERE f_id = 1'
# parse(statement)
