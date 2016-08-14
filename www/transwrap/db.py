#!/usr/bin/env python
# encoding: utf-8
import threading, time, uuid,functools, logging as log

class Dict(dict):

    """Docstring for Dict. """

    def __init__(self, names=(), values=(), **kw):
        """TODO: to be defined1.

        :names(): TODO
        :values(): TODO
        :**kw: TODO

        """
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v


    def __getattr__(self, key):
        """TODO: Docstring for __getattr__.

        :key: TODO
        :returns: TODO

        """
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)
    def __setattr__(self, key , value):
        """TODO: Docstring for __setattr__.

        :value: TODO
        :returns: TODO

        """
        self[key] = value

def _profiling(start, sql=''):
    """TODO: Docstring for _profiling.

    :start: TODO
    :sql: TODO
    :returns: TODO

    """
    t = time.time() - start
    if t > 01:
        log.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        log.info('[PROFILING] [DB] %s: %s' % (t, sql))



def next_id(t=None):
    """TODO: Docstring for next_id.

    :t: TODO
    :returns: TODO

    """
    if t is None:
        t = time.time()
    return "%015d%s000" % (int(t*1000), uuid.uuid4.hex)

class DBError(Exception):

    pass
class MultiCloumnError(DBError):

    pass

class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            log.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()
    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        """TODO: Docstring for cleanup.
        :returns: TODO

        """
        if self.connection:
            connection = self.connection
            self.connection = None
            log.info('close connection <%s>...' % hex(id(connection)))
            connection.close()


engine = None
class _Engine(object):

    """Docstring for _Engine. """

    def __init__(self, connect):
        """TODO: to be defined1.

        :connect: TODO

        """
        self._connect = connect

    def connect(self):
        """TODO: Docstring for connect.
        :returns: TODO

        """
        return self._connect()


def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda: mysql.connector.connect(**params))
    # test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

class _DbCtx(threading.local):

    """Docstring for _DbCtx. """

    def __init__(self):
        """TODO: to be defined1. """

        self.connection = None
        self.transactions = 0

    def is_init(self):
        """TODO: Docstring for is_init.
        :returns: TODO

        """
        return not self.connection is None

    def init(self):
        """TODO: Docstring for init.
        :returns: TODO

        """
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        """TODO: Docstring for cleanup.
        :returns: TODO

        """
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        """TODO: Docstring for cursor.
        :returns: TODO

        """
        return self.connection.cursor()

_db_ctx = _DbCtx()

class _ConnectionCtx(object):

    """Docstring for _ConnectionCtx. """

    def __enter__(self):
        """TODO: to be defined1. """
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return False

    def __exit__(self):
        """TODO: Docstring for __exit__.
        :returns: TODO

        """
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

    def connection():
        """TODO: Docstring for connection.
        :returns: TODO

        """
        return _ConnectionCtx()

def connection():
    return _ConnectionCtx()

def with_connection(func):
    """TODO: Docstring for with_connection.

    :func: TODO
    :returns: TODO

    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        """TODO: Docstring for _wrapper.

        :*args: TODO
        :**kw: TODO
        :returns: TODO

        """
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper

class _TransactionCtx(object):

    """Docstring for _TransactionCtx. """

    def __enter__(self):
        """TODO: to be defined1. """
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions += 1
        return self

    def __exit__(self, exectype, execvalue, traceback):
        """TODO: Docstring for __exit__.

        :exectype: TODO
        :execvalue: TODO
        :traceback: TODO
        :returns: TODO

        """
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1

        try:
            if _db_ctx.transactions == 0:
                if exectype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup

    def commit(self):
        """TODO: Docstring for commit.
        :returns: TODO

        """
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except:
            _db_ctx.connection.rollback()
            raise

    def rollbak(sel):
        """TODO: Docstring for rollbak.
        :returns: TODO

        """
        global _db_ctx
        _db_ctx.connection.rollbak()

def transaction():
    """TODO: Docstring for transaction.
    :returns: TODO

    """
    return _TransactionCtx()


def with_transaction(func):
    """TODO: Docstring for with_transaction.

    :func: TODO
    :returns: TODO

    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        """TODO: Docstring for _wrapper.

        :*args: TODO
        :**kw: TODO
        :returns: TODO

        """
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper


def _select(sql, first, *args):
    """TODO: Docstring for _select.

    :sql: TODO
    :first: TODO
    :*args: TODO
    :returns: TODO

    """
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    log.info('SQL: %s, ARGS: %s' % (sql, args))

    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names,x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select_one(sql, *args):
    """TODO: Docstring for select_one.

    :sql: TODO
    :*args: TODO
    :returns: TODO

    """
    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
    """TODO: Docstring for select_int.

    :sql: TODO
    :*args: TODO
    :returns: TODO

    """
    rs = _select(sql, True, *args)
    if len(rs) != 1:
        raise MultiCloumnError('Expect only one column')
    return rs.values[0]


@with_connection
def select(sql, *args):
    """TODO: Docstring for select.

    :sql: TODO
    :*args: TODO
    :returns: TODO

    """
    return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
    """TODO: Docstring for _update.

    :sql: TODO
    :*args: TODO
    :returns: TODO

    """
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')

    log.info('SQL: %s, ARGS: %s' % (sql, args))

    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions==0:
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()


def insert(table, **kw):
    """TODO: Docstring for insert.

    :table: TODO
    :**kw: TODO
    :returns: TODO

    """
    cols, args = zip(*kw.iteritems)
    sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['%s' % col for col in cols]), ','.join(['%s' % arg for arg in args]))
    return _update(sql, *args)

def update(sql, *args):
    """TODO: Docstring for update.

    :sql: TODO
    :*args: TODO
    :returns: TODO

    """
    return _update(sql, *args)

