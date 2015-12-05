from daos import ListingDAO
from tornado import httpserver, ioloop, options, web, gen
from tornado.escape import json_decode
import json
import momoko


class Application(web.Application):
    def __init__(self):
        handlers = [
            (r"/create-table", PostgresTableHandler),
            (r"/delete-table", PostgresTableHandler),
            (r"/listing/(\d+)", PostgresListingHandler),
            (r"/listing", PostgresListingHandler)
        ]
        web.Application.__init__(self, handlers)
        dsn = 'dbname=ds_test user=db_test password=test ' \
              'host=localhost port=5432'
        self.db = momoko.Pool(dsn=dsn, size=5)


class PostgresHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("GET", "HEAD", "POST", "DELETE", "PATCH", "PUT", "OPTIONS") 
    @gen.coroutine
    def prepare(self):
        if self.request.headers.get("Content-Type") == "application/json":
            try:
                self.json_args = json_decode(self.request.body)
            except Exception as error:
                self.finish('invalid request')

    @property
    def db(self):
        return self.application.db

# PostgresTableHandler - Controller
# Handling the APIs requests with their corresponding Table Model Methods
class PostgresTableHandler(PostgresHandler):

    @gen.coroutine
    def post(self):
        dao = ListingDAO(self.db)
        cursor = yield (dao.create_table())
        if not cursor.closed:
            self.write('closing cursor')
            cursor.close()
        self.finish()

    @gen.coroutine
    def delete(self):
        dao = ListingDAO(self.db)
        cursor = yield (dao.delete_table())
        if not cursor.closed:
            self.write('closing cursor')
            cursor.close()
        self.finish()


# PostgresListingHandler - Controller
# Handling the APIs requests with their corresponding Listing Model Methods
class PostgresListingHandler(PostgresHandler):
    @gen.coroutine
    def get(self, id=None):
        dao = ListingDAO(self.db)
        if not id:
            dict_result = yield (dao.get_list())
        else:
            dict_result = yield (dao.get(id))
        self.write(json.dumps(dict_result))
        self.finish()

    @gen.coroutine
    def post(self):
        dao = ListingDAO(self.db)
        cursor = yield (dao.create())
        if not cursor.closed:
            self.write('closing cursor')
            cursor.close()
        self.finish()

    @gen.coroutine
    def put(self, id=None):
        if not hasattr(self, 'json_args'):
            self.write('invalid request')
            self.finish()
        else:
            dao = ListingDAO(self.db)
            if id:
                result = yield (dao.update(id, data=self.json_args))
                dict_result = yield (dao.get(id))
                self.write(json.dumps(dict_result))
            else:
                self.write('invalid listing')
            self.finish()

    @gen.coroutine
    def delete(self, id=None):
        if id:
            dao = ListingDAO(self.db)
            result = yield (dao.delete(id))
            self.write('listing deleted')
        else:
            self.write('invalid listing')
        self.finish()



def main():
    http_server = httpserver.HTTPServer(Application())
    PORT = 9000
    print("serving at port", PORT)
    http_server.listen(PORT)
    ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
