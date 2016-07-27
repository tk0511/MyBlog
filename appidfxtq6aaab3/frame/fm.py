#-*- coding:utf-8 -*-

import functools, time

def get(route, *demand):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        wrapper.__method__ = 'GET'
        wrapper.__route__ = route
        wrapper.__demand__ = demand
        return wrapper
    return decorator

def post(route, *demand):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        wrapper.__method__ = 'POST'
        wrapper.__route__ = route
        wrapper.__demand__ = demand
        return wrapper
    return decorator

def qs(name = '__main__'):
    module = __import__(name, globals(), locals())
    for fun_name in filter(lambda fn: not fn.startswith('_') and getattr(getattr(module, fn), '__method__', None), dir(module)):
        _add_route(getattr(module, fun_name))
    return _app

#-----------------In-----------------
_ROUTE = {'GET':{},'POST':{}}

def _app(env, start_response):
    timer = time.time() #debuger--------------
    status = '200 OK'
    headers = {'Content-Type':'text/html;charset=utf-8','Sever':'dadada'}
    fun = _getfun(env['REQUEST_METHOD'].upper(), env['PATH_INFO'] if env.get('PATH_INFO') else env['REQUEST_URI'])
    if not fun:
        start_response('404 Not Found', headers.items())
        return str(env)
    
    result = fun(*map(lambda v: env.get(v.upper()), fun.__demand__)) if fun.__demand__ else fun()
    if result.get('header'): headers.update(result['header'])
    if result.get('status'): status = result['status']
    start_response(status, headers.items())
    return result.get('data', '')+str(time.time()-timer)#debugger-------------

def _getfun(method, uri):
    global _ROUTE
    target = _ROUTE[method]
    for node in uri.split('?')[0].split('/')[1:]:
        if type(target)==dict:
            target = target.get(node) if target.get(node) else target.get('*')
        else:
            return None
    return target if callable(target) else None

def _add_route(fun):
    global _ROUTE
    def follow(d, p, f):
        if len(p)>1:
            if not d.get(p[0]): d[p[0]]={}
            follow(d[p[0]], p[1:], f)
        else:
            if d.get(p[0]): raise ValueError('Path conflict between %s and %s'% (f,d[p[0]]))
            d[p[0]] = f
    follow(_ROUTE[fun.__method__], fun.__route__.split('/')[1:], fun)