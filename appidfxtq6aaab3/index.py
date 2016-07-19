#-*- coding:utf-8 -*-


import os
import jinja2
import time
import db.orm as db
import webapp2
import logging
from conf import conf
import logging.config

if os.path.exists(os.path.join(os.getcwd(),'LocalSever.py')):
    LOCAL_TEST = True
else:
    LOCAL_TEST = False


class Handler(webapp2.RequestHandler):
    def write(self, *a, **kw):
        self.response.out.write(*a, **kw)

    def render_str(self, template, **params):
        t = JINJA_ENV.get_template(template)
        return t.render(params)

    def render(self, template, **kw):
        self.write(self.render_str(template, **kw))

def connect_sql():
    try:
        db.connect(conf.poi.user,conf.poi.passwd,conf.poi.db,conf.poi.host,conf.poi.port)
    except:
        LOGGER.error('db connection failed')
        raise
    return db

class MainPage(Handler):
    def get(self):
        global LOGGER
        timer = time.time()
        items = self.request.get_all("item")
        self.render("Index.html", items = items)
        LOGGER.info(time.time() - timer)


def setup():
    #Setup Logger
    global LOGGER
    if LOCAL_TEST:
        logging.config.fileConfig("conf/logging_local.conf")
    else:
        logging.config.fileConfig("conf/logging.conf")
    LOGGER = logging.getLogger()
    
    #Setup Jinja
    global JINJA_ENV
    template_dir = os.path.join(os.path.dirname(__file__),"Templates")
    JINJA_ENV = jinja2.Environment(loader = jinja2.FileSystemLoader(template_dir), autoescape = True)

    #Connect to mySQL data base'
    global DB
    DB = connect_sql()

    LOGGER.info("Initialize sucess!")
    return webapp2.WSGIApplication([('/',MainPage),],debug=True)


# Run
if LOCAL_TEST:
    application = setup()
else:
    from bae.core.wsgi import WSGIApplication
    application = WSGIApplication(setup())
