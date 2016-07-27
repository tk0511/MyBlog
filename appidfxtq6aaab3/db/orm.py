#-*- coding: utf-8 -*-
import re
import db
from collections import namedtuple

#----------Fields-----------
def int_field(val):
    if type(val) == int and val >= -2147483648 and val <= 2147483647:
        return val
    else:
        raise ValueError('Expect int, got %s %s'%(val,type(val)))

def str_field(val):
    if type(val) == str:
        return val
    else:
        raise ValueError('Expect str, got %s %s'%(val,type(val)))

def bool_field(val):
    if type(val) == bool:
        return val
    else:
        raise ValueError('Expect bool, got %s %s'%(val,type(val)))

def time_field(val):
    if type(val) == float:
        return val
    else:
        raise ValueError('Expect float, got %s %s'%(val,type(val)))

#-------Operation-------
def new_type(type_name, type_sql, type_verifier = None, **kwargs):
    global _TYPE, _TYPE_VERIFIER, _TYPE_ATTRS
    _TYPE[type_name] = type_sql
    _TYPE_VERIFIER[type_name] = type_verifier
    _TYPE_ATTRS[type_name] = {'updateable' : True}
    for key in kwargs.keys():
        _TYPE_ATTRS[type_name][key] = kwargs[key]

def qcr(table_name, primary_key, *columns):
    if execute("select * from information_schema.TABLES where TABLE_ScHEMA='%s' and table_name='%s';" % (db.ENGINE.connect().database,table_name)):
        return None

    global _TABLE_INFO, _TYPE, _TYPE_VERIFIER, _TABLE_PRIMARY, _ColumnInfo
    sql = 'CREATE TABLE %s(' % table_name
    _TABLE_INFO[table_name] = []
    _TABLE_PRIMARY[table_name] = primary_key
    for column in columns:
        info = _VAL_REGEX.match(column).groups()
        _TABLE_INFO[table_name].append(_ColumnInfo(info[0], info[1], info[3], info[4], _TYPE_VERIFIER.get(info[1].lower() if info[1] else None ,None)))
        sql += ' '.join((info[0], _TYPE.get(info[1].lower(), info[1]).upper(), info[-1].upper(), ','))
    if primary_key:
        sql += 'PRIMARY KEY ( %s ));' % primary_key
    else:
        sql = sql[:-2] + (')if not exists %s;' % table_name)
    execute(sql)
    return 1

def qin(table,values = None, **kwargs):
    'Raise ValueError when got incorrect value'
    global _TABLE_INFO
    if values:
        if type(values) == dict :
            val_dict = {}
            for val in zip(map(lambda v:v.name, _TABLE_INFO[table]), values.values(), map(lambda v:v.default, _TABLE_INFO[table]), map(lambda v:v.type_verifier, _TABLE_INFO[table])):
                val_dict[val[0]] = (val[-1](val[1]) if val[1] else val[-1](val[2])) if val[-1] else val[2] if not val[1] else val[1]
            return db.insert.one(table, val_dict)

        elif type(values[0]) == dict and len(values[0]) == len(_TABLE_INFO.get(table,())):
            val_dict_list = []
            for val_dict in values:
                for column in _TABLE_INFO[table]:
                    if val_dict.get(column.name, None):
                        if column.default and not val_dict[column.name]:
                            val_dict[column.name] = column.default
                        if column.type_verifier:
                            val_dict[column.name] = column.type_verifier(val_dict[column.name])
                val_dict_list.append(val_dict)
            return db.insert.many(table, *val_dict_list)

        elif (type(values[0]) == list or type(values[0]) == tuple) and len(values) == len(_TABLE_INFO.get(table,())):
            val_dict_list = []
            for sub_val in values:
                val_dict = {}
                for val in zip(map(lambda v:v.name, _TABLE_INFO[table]), values, map(lambda v:v.default, _TABLE_INFO[table]), map(lambda v:v.type_verifier, _TABLE_INFO[table])):
                    val_dict[val[0]] = (val[-1](val[1]) if val[1] else val[-1](val[2])) if val[-1] else val[2] if not val[1] else val[1]
                val_dict_list.append(val_dict)
            return db.insert.many(table, *val_dict_list)

        elif len(values) == len(_TABLE_INFO.get(table,())):
            val_dict = {}
            for val in zip(map(lambda v:v.name, _TABLE_INFO[table]), values, map(lambda v:v.default, _TABLE_INFO[table]), map(lambda v:v.type_verifier, _TABLE_INFO[table])):
                val_dict[val[0]] = (val[-1](val[1]) if val[1] else val[-1](val[2])) if val[-1] else val[2] if not val[1] else val[1]
            return db.insert.one(table, val_dict)

    elif kwargs:
        val_dict = kwargs
        for column in _TABLE_INFO[table]:
            if val_dict.get(column.name, None):
                if column.default and not val_dict[column.name]:
                    val_dict[column.name] = column.default
                if column.type_verifier:
                    val_dict[column.name] = column.type_verifier(val_dict[column.name])
        return db.insert.one(table, val_dict)

def qsl(table, **kwargs):
    if kwargs:
        result = db.select.all(table, kwargs)
        return result if result else None

def qup(table, primary_val, **kwargs):
    global _TABLE_INFO, _TYPE_ATTRS, _TABLE_PRIMARY
    if kwargs:
        for column in _TABLE_INFO[table]:
            if kwargs.get(column.name, None) and (not _TYPE_ATTRS.get(column.type,{}).get('updateable', True)):
                return None
        return db.update.one(table,{_TABLE_PRIMARY[table]:primary_val}, kwargs)

def qde(table, primary_val):
    'delete one record by primary value'
    global _TABLE_PRIMARY
    return db.delete.one(table, {_TABLE_PRIMARY[table]:primary_val})

_ColumnInfo = namedtuple('_ColumnInfo','name type default extra type_verifier')
_VAL_REGEX = re.compile('([^ ]*) ([^ ]*) ?(default ?= ?)?(?(3)([^ ]*)|) ?(.*)')
_TYPE_VERIFIER = {'account': str_field}
_TYPE_ATTRS = {'account':{}}
_TYPE = {'account':'char(20)'}
_TABLE_PRIMARY = {}
_TABLE_INFO = {}
execute = db.execute
connect = db.deploy_engine

#----------TEST-----------
if __name__ == '__main__':
    try:
        db.deploy_engine('root', '4444', 'test', '127.0.0.1', 3306)
    except:
        print('Failed to deploy enfine')
        raise
    db.execute('drop table if exists test_u')

    def test():
        '''

        >>> qcr('test_u','id','id int default = 12 not null', 'name account default = anonymous')
        1
        >>> qcr('test_u','id','id int default = 12 not null', 'name char(10)')
        >>> qin('test_u',(10, 'nana'))
        1
        >>> qin('test_u',{'id':11, 'name':'dada'})
        1
        >>> qin('test_u',id = 12, name = 'wawa')
        1
        >>> qin('test_u',({'id':13, 'name':'lala'},{'id':14,'name':'lala'}))
        2
        >>> qin('test_u',(15,None))
        1
        >>> qsl('test_u',id = 11)
        [{u'id': 11, u'name': u'dada'}]
        >>> qup('test_u',11,name = 'papa')
        1
        >>> qsl('test_u',id = 11)
        [{u'id': 11, u'name': u'papa'}]
        >>> qde('test_u',11)
        1
        >>> qsl('test_u',id = 11)
        '''
    import doctest
    doctest.testmod()