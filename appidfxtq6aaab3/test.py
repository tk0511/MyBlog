# -*- coding: utf-8 -*-
'''
print '----------------A----------------'
str_ = '今晚6:00出发'
print(filter(lambda c:~ord(c)&0x80, str_))


print '----------------B----------------'
def count(n):
    while True:
        newval = yield n
        if newval is not None:
            n *= newval
        else:
            n *= 3

c = count(1.)
for _ in range(10):
    print(c.send(c.next()))
'''

import re

def cr_auto_next(func):
    def start(*args,**kwargs):  
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

@cr_auto_next
def guider(**cr_map):
    testament = []
    while True:
        data = yield testament
        for cr in cr_map[data[0]]:
            testament.append(cr.send(data[1:]))

@cr_auto_next
def route(event, next_route):
    testament = None
    while True:
        data = yield testament
        testament = next_route.send(event(data))

@cr_auto_next
def end(event):
    testament = None
    while True:
        data = yield testament
        testament = event(data)

def int_field(val):
    if type(val) == int and val >= -2147483648 and val <= 2147483647:
        return val

def str_field(val):
    if type(val) == str:
        return val

def bool_field(val):
    if type(val) == bool:
        return val

def time_field(val):
    if type(val) == float:
        return val

def add_table(table_name,*columns):
    pass

print int_field('a')

('table', 'operation', 'value')


ENTRY = guider()

'''
create('users', 'id int auto',  )
'''

#___________test_____________
from collections import namedtuple
import time

Val = namedtuple('Val', 'type value')

def x(**kwarg):
    return 0

def loop(str, i):
    exec(str*i)

import compiler
li = []
cr = route(x,route(x,end(x)))
_aaa = Val('int', 15)
#exec('''def funct(i): a=0;%s return a;'''%('x();'*1000000))
timer = time.time();
for _ in range(100000):
    type('xx')
print time.time() - timer
'''
0.38
nothing 3.39
list 5.64
tuple 5.31
dict 5.60
namedtuple 13.55
'''