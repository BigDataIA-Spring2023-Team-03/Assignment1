from S3Util import S3Util
import sqlite3

class DbUtil:
    conn = None

    def __init__(self, db_file_name):
        self.conn = sqlite3.connect(db_file_name)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, *columns):
        query = '''CREATE TABLE IF NOT EXISTS {} (
                                    {}
                                );'''.format(table_name, ', '.join(columns))
        self.cursor.execute(query)
        self.conn.commit()

    def insert(self, table_name, column_names, list_of_tuples):
        util.conn.executemany('INSERT INTO {} ({}) VALUES ({})'
                              .format(table_name,
                                      ', '.join([i for i in column_names]),
                                      ', '.join(['?' for _ in range(len(column_names))])),
                              list_of_tuples)
        util.conn.commit()

    def filter(self, table_name, req_value, **input_values):
        query = '''SELECT DISTINCT {} from {}  WHERE'''.format(req_value, table_name)
        for i in input_values.keys():
            query += ' {} = ? AND'.format(i)
        if query.endswith('AND'):
            query = query[:-4]
        self.cursor.execute(query, tuple(input_values.values()))
        l = self.cursor.fetchall()
        return sorted([x[0] for x in l])


if __name__ == '__main__':
    column_names_geos = {'id': 'INTEGER PRIMARY KEY AUTOINCREMENT', 'product': 'TEXT', 'year': 'TEXT', 'day_of_year': 'TEXT', 'hour': 'TEXT'}
    column_names_nexrad = {'id': 'INTEGER PRIMARY KEY AUTOINCREMENT', 'year': 'TEXT', 'month': 'TEXT', 'day': 'TEXT', 'station': 'TEXT'}
    db_file_name = 'metadata.db'
    bucket_name_geos = 'noaa-goes18'
    bucket_name_nexrad = 'noaa-nexrad-level2'
    table_name_geos = 'geos18'
    table_name_nexrad = 'nexrad'
    resource_name = 's3'

    # s3util = S3Util(resource_name, bucket_name_nexrad)
    s3util = S3Util(resource_name, bucket_name_geos)
    pages = s3util.get_pages(prefix='ABI-L1b-RadC')
    # pages = s3util.get_pages(prefix='2023')
    util = DbUtil(db_file_name=db_file_name)
    try:
        # GEOS
        # creation of table
        columns_to_create = [key + ' ' + value for key, value in column_names_geos.items()]
        column_keys = list(column_names_geos.keys())
        util.create_table(table_name_geos, *columns_to_create)

        # insertion of rows
        rows = set()
        for page in pages:
            for obj in page['Contents']:
                levels = obj['Key'].split('/')
                if len(levels) == 5:
                    product = levels[0]
                    year = levels[1]
                    day_of_year = levels[2]
                    hour = levels[3]
                    rows.add((product, year, day_of_year, hour))
                if len(rows) % 1000 == 0:
                    util.insert(table_name_geos, column_keys[1:], rows)
                    rows = set()
        if rows:
            util.insert(table_name_geos, column_keys[1:], rows)

        cursor = util.conn.cursor()

        # filtering of rows
        print(util.filter(table_name_geos, '', product='ABI-L1b-RadC', year='2022', day_of_year='209'))
        print(util.filter(table_name_geos, 'day_of_year', product='ABI-L1b-RadC', year='2022'))
        print(util.filter(table_name_geos, 'year', product='ABI-L1b-RadC'))

        # NEXRAD
        # creation of table
        # columns_to_create = [key + ' ' + value for key, value in column_names_nexrad.items()]
        # column_keys = list(column_names_nexrad.keys())
        # util.create_table(table_name_nexrad, *columns_to_create)

        # insertion of rows
        # rows = set()
        # for page in pages:
        #     for obj in page['Contents']:
        #         levels = obj['Key'].split('/')
        #         if len(levels) == 5:
        #             year = levels[0]
        #             month = levels[1]
        #             day = levels[2]
        #             station = levels[3]
        #             rows.add((year, month, day, station))
        #         if len(rows) % 1000 == 0:
        #             util.insert(table_name_nexrad, column_keys[1:], rows)
        #             rows = set()
        # if rows:
        #     util.insert(table_name_nexrad, column_keys[1:], rows)
        #
        # cursor = util.conn.cursor()

        # filtering of rows
        # print(util.filter(table_name_nexrad, 'month', year='2022'))
        # print(util.filter(table_name_nexrad, 'day', year='2022', month='11'))
        # print(util.filter(table_name_nexrad, 'station', year='2022', month='11', day='30'))
    finally:
        util.conn.close()
