from tornado import gen

import momoko
import string
import random

class ListingDAO(object):
    def __init__(self, db):
        self.db = db

    #Helper Method for generating a random string of length 10
    def _get_random_str(self, size=10):
        return ''.join(random.choice(string.ascii_uppercase + string.digits)
                       for x in range(size))

    #Helper Method for generating a random integer between 0 - 1000
    def _get_random_int(self): 
        return random.randint(0, 1000)

    #Helper Method for generating a random digits string of length 7
    def _get_random_postal_code(self, size=7): 
        return ''.join(random.choice(string.digits)
                        for x in range(size))

    #Helper Method for generating a random status from status_array
    def _get_random_status(self): 
        status_array = ['active', 'closed', 'deleted']
        return random.choice(status_array)

    #Helper Method for generating a random listing type from listing_types array
    def _get_random_listing_types(self): 
        listing_types = ['rent', 'sale']
        return random.choice(listing_types)

    ## Models for APIS
    # Get Method for retrieving the listing
    # Query Params - :id
    @gen.coroutine
    def get(self, id):
        sql = """
            SELECT user_id, listing_type, postal_code, price, status
            FROM listings
            WHERE id=%s
        """
        cursor = yield momoko.Op(self.db.execute, sql, (id,))
        desc = cursor.description
        result = [dict(zip([col[0] for col in desc], row))
                         for row in cursor.fetchall()]

        cursor.close()
        yield result

    # Get Method for retrieving the all listings
    @gen.coroutine
    def get_list(self):
        sql = """
            SELECT user_id, listing_type, postal_code, price, status
            FROM listings
        """
        cursor = yield momoko.Op(self.db.execute, sql)
        desc = cursor.description
        result = [dict(zip([col[0] for col in desc], row))
                         for row in cursor.fetchall()]

        cursor.close()
        yield result

    # Post Method for creating listing
    @gen.coroutine
    def create(self):
        sql = """
            INSERT INTO listings (user_id, listing_type, postal_code, price, status)
            VALUES (%s, %s, %s, %s, %s )
        """
        user_id = self._get_random_int()
        status = self._get_random_status()
        postal_code = self._get_random_postal_code()
        listing_type = self._get_random_listing_types()
        price = self._get_random_int()
        
        cursor = yield momoko.Op(self.db.execute, sql, (user_id, listing_type, postal_code, price, status))
        yield cursor


    # Put Method for updating listings
    # Query Params - :id
    @gen.coroutine
    def update(self, id, data={}):
        fields = ''
        for key in data.keys():
            fields += '{0}=%s,'.format(key)

        sql = """
            UPDATE listings
            SET {0}
            WHERE id=%s
        """.format(fields[0:-1])
        params = list(data.values())
        params.append(id)
        cursor = yield momoko.Op(self.db.execute, sql, params)
        yield cursor


    # DeleteTable for deleting table
    @gen.coroutine
    def delete_table(self):
        sql = """
            DROP TABLE IF EXISTS listings;
            DROP SEQUENCE IF EXISTS listing_id;
        """
        cursor = yield momoko.Op(self.db.execute, sql)
        yield cursor

    # Delete Method for deleting a listing row from table
    @gen.coroutine
    def delete(self, id):
        sql = """
            DELETE
            FROM listings
            WHERE id=%s
        """
        cursor = yield momoko.Op(self.db.execute, sql, (id,))
        cursor.close()
        yield ''

    # Create Table Method - which creates a table according to hard coded schema
    @gen.coroutine
    def create_table(self, callback=None):
        sql = """
            CREATE SEQUENCE  listing_id;
            CREATE TABLE IF NOT EXISTS listings (
                id integer PRIMARY KEY DEFAULT nextval('listing_id') ,
                user_id  integer,
                listing_type varchar(80),
                postal_code varchar(80),
                price integer,
                status varchar(80)
            );
            ALTER SEQUENCE listing_id OWNED BY listings.id;
        """
        cursor = yield momoko.Op(self.db.execute, sql)
        yield cursor