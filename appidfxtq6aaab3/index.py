#-*- coding:utf-8 -*-

import os
import conf
import jinja2
import webapp2
import MySQLdb
import logging
import logging.config



class Handler(webapp2.RequestHandler):
    def write(self, *a, **kw):
        self.response.out.write(*a, **kw)

    def render_str(self, template, **params):
        t = JINJA_ENV.get_template(template)
        return t.render(params)

    def render(self, template, **kw):
        self.write(self.render_str(template, **kw))

def connect_sql():
    mydb = MySQLdb.connect(
        host = conf.poi.host,
        port = conf.poi.port,
        user = conf.poi.user,
        passwd = conf.poi.passwd,
        db = conf.poi.db)
    return mydb

class MainPage(Handler):
    def get(self):
        items = self.request.get_all("item")
        self.render("Index.html", items = items)


def setup():
    try:
        #Setup Logger
        global LOGGER
        logging.config.fileConfig("logging.conf")
        LOGGER = logging.getLogger()

        #Setup Jinja
        global JINJA_ENV
        template_dir = os.path.join(os.path.dirname(__file__),"Templates")
        JINJA_ENV = jinja2.Environment(loader = jinja2.FileSystemLoader(template_dir), autoescape = True)

        #Connect to mySQL data base'
        global DB
        DB = connect_sql()
    except:
        LOGGER.fatal('Initialize failed!')
        raise

    LOGGER.info("Initialize sucess!")
    return webapp2.WSGIApplication([('/',MainPage),],debug=True)


# Run
try:
    from bae.core.wsgi import WSGIApplication
    application = WSGIApplication(setup())
except:
    application = setup

