#-*- coding:utf-8 -*-
import urllib

def query_str_reader(str, decode = True):
    result = {}
    if '=' in str:
        for pair in str.split('&'):
            key, val = pair.split('=')
            if result.get(key):
                result[key].append(urllib.unquote(val).decode('utf-8') if decode else val)
            else:
                result[key] = [urllib.unquote(val).decode('utf-8') if decode else val]
    return result
