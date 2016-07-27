#-*- coding:utf-8 -*-

import os, jinja2, logging
import db.orm as db
from conf import conf
import logging.config
from frame.fm import *
from frame import tools

#Handles
#--------------------------------------
@get('/')
def index():
    tmp = open(r'Templates/index.html', 'r').read()
    return {'data':tmp}


@get('/listmaker', 'QUERY_STRING')
def listmaker(q_str):
    global JINJA_ENV
    tmp = JINJA_ENV.get_template('listmaker.html')
    val_dict = tools.query_str_reader(q_str)
    return {'data':tmp.render(items = val_dict.get('item')).encode('utf-8')}


#Run
#--------------------------------------
if __name__ == '__main__':
    def sever_local(port):
        from wsgiref.simple_server import make_server
        httpd = make_server('', port, qs())
        print("Sever runing at http://localhost:%s" % port)
        httpd.serve_forever()

    JINJA_ENV = jinja2.Environment(loader = jinja2.FileSystemLoader(os.path.join(os.path.dirname(__file__),"Templates")), autoescape = True)
    DB = db.connect(conf.local.user,conf.local.passwd,conf.local.db,conf.local.host,conf.local.port)
    logging.config.fileConfig("conf/logging_local.conf")
    LOGGER = logging.getLogger()
    LOGGER.info('init sucess!')
    sever_local(8080)
else:
    JINJA_ENV = jinja2.Environment(loader = jinja2.FileSystemLoader(os.path.join(os.path.dirname(__file__),"Templates")), autoescape = True)
    DB = db.connect(conf.poi.user,conf.poi.passwd,conf.poi.db,conf.poi.host,conf.poi.port)
    logging.config.fileConfig("conf/logging.conf")
    LOGGER = logging.getLogger()
    LOGGER.info('init sucess!')
    application = qs(__name__)
