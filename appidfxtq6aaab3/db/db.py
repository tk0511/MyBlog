#-*- coding: utf-8 -*-

import time, functools, threading


class Record(dict):
    def __init__(self, column_names, values, **kw):
        super(Record, self).__init__(**kw)
        if values is None:
            return None
        else:
            for n, v in zip(column_names, values):
                self[n] = v

    def __getattr__(self, colunm_name):
        try:
            return self[colunm_name]
        except KeyError:
            raise AttributeError(r"No attribute named '%s'" % key)

    def __setattr__(self, column_name, value):
        self[column_name] = value

class DBError(Exception):
    pass

class _LazyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            self.connection = ENGINE.connect()
        return self.connection.cursor()
    
    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            self.connection.close()
            self.connection = None

class _DBContext(threading.local):
    def __init__(self):
        self.connection = None
        self.transaction = 0

    def inited(self):
        return not self.connection is None

    def init(self):
        self.connection = _LazyConnection()
        self.transaction = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()

def deploy_engine(user, password, database, host , port , **kw):
    import mysql.connector
    global ENGINE
    if ENGINE is not None:
        raise DBError("Engine already initialized!")
    params = dict(user=user, password=password, database=database, host=host, port=port,
                  charset = 'utf8',
                  use_unicode = True,
                  collation = 'utf8_general_ci',
                  autocommit = False,
                  buffered = True
                  )
    params.update(kw)
    ENGINE = _Engine(lambda: mysql.connector.connect(**params))

class _ConnectionContext(object):
    def __enter__(self):
        global _DB_CONTEXT
        self.need_cleanup = False
        if not _DB_CONTEXT.inited():
            _DB_CONTEXT.init()
            self.need_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _DB_CONTEXT
        if self.need_cleanup:
            _DB_CONTEXT.cleanup()

def connection():
    return _ConnectionContext()

def with_connection(fun):
    @functools.wraps(fun)
    def _wrapper(*args, **kw):
        with _ConnectionContext():
            return fun(*args, **kw)
    return _wrapper

class _TransactionContext(object):
    def __enter__(self):
        global _DB_CONTEXT
        self.need_cleanup = False
        if not _DB_CONTEXT.inited():
            _DB_CONTEXT.init()
            self.need_cleanup = True
        _DB_CONTEXT.transaction += 1
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _DB_CONTEXT
        _DB_CONTEXT.transaction -= 1
        try:
            if _DB_CONTEXT.transaction == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.need_cleanup:
                _DB_CONTEXT.cleanup()

    def commit(self):
        global _DB_CONTEXT
        try:
            _DB_CONTEXT.connection.commit()
        except:
            _DB_CONTEXT.connection.rollback()
            raise

    def rollback(self):
        global _DB_CONTEXT
        _DB_CONTEXT.connection.rollback()

def transaction():
    '''
    
    >>> def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert.one('user', **u)
    ...     r = update.one('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> with transaction():
    ...     update_profile(900301, 'Python', False)
    >>> select.one('select * from user where id=?', 900301).name
    u'Python'
    >>> with transaction():
    ...     update_profile(900302, 'Ruby', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    >>> select.one('select * from user where id=?', 900302)
    {}
    '''
    return _TransactionContext()

def with_transaction(fun):
    '''
    A decorator that makes function around transaction.

    >>> @with_transaction
    ... def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert.one('user', **u)
    ...     r = update.one('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> update_profile(8080, 'Julia', False)
    >>> select.one('select * from user where id=?', 8080).passwd
    u'JULIA'
    >>> update_profile(9090, 'Robert', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    >>> select.one('select * from user where id=?', 9090)
    {}
    '''
    @functools.wraps(fun)
    def _wrapper(*args, **kw):
        with _TransactionContext():
            return fun(*args, **kw)
    return _wrapper

def _column_names(cursor):
    if cursor.description:
        return [x[0] for x in cursor.description]
    else:
        return None

class _DBExecute(object):
    def __init__(self, commit, cmd, *args):
        self.cmd = cmd.replace('?', '%s')
        self.args = args
        self.commit = commit

    def __enter__(self):
        global _DB_CONTEXT
        self.cursor = _DB_CONTEXT.connection.cursor()
        self.cursor.execute(self.cmd, self.args)
        return self.cursor

    def __exit__(self, exctype, excvalue, traceback):
        global _DB_CONTEXT
        if self.commit and _DB_CONTEXT.transaction == 0:
            _DB_CONTEXT.connection.commit()
        if self.cursor:
            self.cursor.close()

class Select(object):
    @with_connection
    def one(self, table, condition):
        cmd,values = self.build(table, condition)
        with _DBExecute(False, cmd, *values) as cursor:
            result = Record(_column_names(cursor), cursor.fetchone())
            if cursor.fetchone() is None:
                return result
            else:
                return None

    @with_connection
    def first(self, table, condition):
        cmd,values = self.build(table, condition)
        with _DBExecute(False, cmd, *values) as cursor:
            return Record(_column_names(cursor), cursor.fetchone())

    @with_connection
    def all(self, table, condition):
        cmd,values = self.build(table, condition)
        with _DBExecute(False, cmd, *values) as cursor:
            column_names = _column_names(cursor)
            return [Record(column_names, values) for values in cursor.fetchall()]

    def build(self, table, condition):
        column_names, values = zip(*condition.iteritems())
        return ('select * from %s where %s' % (table, ' and '.join(map(lambda v:'%s=?'%v, column_names))), values)

class Insert(object):
    @with_connection
    def one(self, table, kw):
        '''
        >>> u1 = dict(id=200, name='Wall.E', email='wall.e@test.org', passwd='back-to-earth', last_modified=11)
        >>> u2 = dict(id=201, name='Eva', email='eva@test.org', passwd='back-to-earth', last_modified=10)
        >>> u3 = dict(id=202, name='lala', email='lala@test.org', passwd='back-to-earth', last_modified=9)
        >>> u4 = dict(id=203, name='baba', email='baba@test.org', passwd='back-to-earth', last_modified=9)
        >>> u5 = dict(id=204, name='kaka', email='kaka@test.org', passwd='back-to-earth', last_modified=9)
        >>> insert.one('user', **u1)
        1
        >>> insert.one('user', **u2)
        1
        >>> L = select.one('select * from user where id=?', 900900900)
        >>> L
        {}
        >>> L = select.one('select * from user where id=?', 200)
        >>> L
        {u'passwd': u'back-to-earth', u'email': u'wall.e@test.org', u'last_modified': 11.0, u'id': 200, u'name': u'Wall.E'}
        >>> L = select.all('select * from user where passwd=? order by id desc', 'back-to-earth')
        >>> L
        [{u'passwd': u'back-to-earth', u'email': u'eva@test.org', u'last_modified': 10.0, u'id': 201, u'name': u'Eva'}, {u'passwd': u'back-to-earth', u'email': u'wall.e@test.org', u'last_modified': 11.0, u'id': 200, u'name': u'Wall.E'}]
        >>> insert.many('user', u3, u4, u5)
        3
        '''
        column_names, values = zip(*kw.iteritems())
        cmd = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % name for name in column_names]), ','.join(['?' for i in range(len(column_names))]))
        with _DBExecute(True, cmd, *values) as cursor:
            return cursor.rowcount

    @with_transaction
    def many(self, table, *args):
        global _DB_CONTEXT
        if isinstance(args[0], dict):
            column_names = args[0].keys()
        else:
            raise DBError('Input must be dict')
        cmd = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % name for name in column_names]), ','.join(['%s' for i in range(len(column_names))]))
        cursor = _DB_CONTEXT.connection.cursor()
        count = 0
        for kv in args:
            cursor.execute(cmd, tuple(kv[name] for name in column_names))
            count += cursor.rowcount
        return count

class Update(object):
    @with_connection
    def one(self, table, condition, kw):
        cmd, values = self.build(table, condition, kw)
        with _DBExecute(True, cmd, *values) as cursor:
            return cursor.rowcount

    def build(self, table, condition, vals):
        column_names, values = zip(*vals.iteritems())
        return ('update %s set %s where %s'%(table, ' and '.join(map(lambda v:'%s=?'%v, column_names)), ' and '.join(map(lambda v:'%s=%s'%v, condition.iteritems()))), values)

class Delete(object):
    @with_connection
    def one(self, table, condition):
        global _DB_CONTEXT
        cmd,values = self.build(table, condition)
        with _DBExecute(True, cmd, *values) as cursor:
            result = Record(_column_names(cursor), cursor.fetchone())
            if cursor.rowcount == 1:
                return 1
            else:
                _DB_CONTEXT.connection.rollback()
                return None

    def build(self, table, condition):
        column_names, values = zip(*condition.iteritems())
        return ('delete from %s where %s' % (table, ' and '.join(map(lambda v:'%s=?'%v, column_names))), values)
        

@with_connection
def execute(cmd):
    with _DBExecute(True, cmd) as cursor:
        try:
            return cursor.fetchall()
        except:
            return cursor.rowcount

update = Update()
insert = Insert()
select = Select()
delete = Delete()
_DB_CONTEXT = _DBContext()
ENGINE = None

if __name__=='__main__':
    try:
        deploy_engine('root', '4444', 'test', '127.0.0.1', 3306)
    except:
        print('Failed to deploy enfine')
    update.one('drop table if exists user')
    update.one('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()
