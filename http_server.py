# coding=utf8

import time, sys, json

import tornado.ioloop
import tornado.web

from paths import get_answer
import query

settings = {
    "debug" : False
}

TEST_MODE = False

class MainPageHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, Server works.")

class APIHandler(tornado.web.RequestHandler):
    def get(self):
        id1 = int(self.get_argument("id1"))
        id2 = int(self.get_argument("id2"))
        print '[TASK: id1=%d, id2=%d]' % (id1, id2)
        
        if query.CLEAR_CACHE == True:
            query.clear_cache()
            
        start = time.time()
        paths, type1, type2 = get_answer(id1, id2)
        end = time.time()
        type1 = 'paper' if type1 == 1 else ('author' if type1 == 2 else 'ERROR')
        type2 = 'paper' if type2 == 1 else ('author' if type2 == 2 else 'ERROR')
        result = 'paths: %4d, time: %4.2fs, %s --> %s' % (len(paths), end - start, type1, type2)
        print '[Finish: %s]' % result
        print 'remote ip:', self.request.remote_ip
        # print 'Query count: ', query.query_cnt
        print 

        if TEST_MODE:
            self.write(result)
        else:
            self.write(json.dumps(paths))


if __name__ == "__main__":
    port = 80
    for arg in sys.argv:
        if arg == 'debug':
            settings["debug"] = True
            query.ENABLE_SHOW_QUERY = True
        elif arg == 'test':
            TEST_MODE = True
        elif arg == 'noclear':
            query.CLEAR_CACHE = False
        elif arg == '5000':
            port = 5000

    print 'Server starting at port %d. DEBUG=%r TEST=%r CLEAR_CACHE=%r' % (port, settings["debug"], TEST_MODE, query.CLEAR_CACHE)
    application = tornado.web.Application([
        (r"/",                      MainPageHandler),
        (r"/version_1",             APIHandler),
    ], **settings)
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()