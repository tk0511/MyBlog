#-*- coding: utf-8 -*-

import time, uuid, functools, threading


def next_id():
    pass

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

_DB_CONTEXT = _DBContext()
ENGINE = None

class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()

def add_engine(user, password, database, host, port, **kw):
    import 
    params = dict(user=user, password=password, database=database, host=host, port=port,
                  charset = 'utf8',
                  use_unicode = True,
                  collation = 'utf8_general_ci',
                  autocommit = False,
                  buffered = True
                  )
    params.update(kw)



w = _LazyConnection()
create_engine(charset = 'asa')