import boto3
import sqlite3
from botocore import UNSIGNED
from botocore.config import Config


class DbUtil:
    conn = None
    def __init__(self, db_file_name):
        self.conn = sqlite3.connect(db_file_name)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, *columns):
        query = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                                    {', '.join(columns)}
                                );'''
        self.cursor.execute(query)
        self.conn.commit()

    def insert(self, table_name, column_names, list_of_tuples):
        print('INSERT INTO {} ({}) VALUES ({})'.format(table_name, ', '.join([i for i in column_names]), ', '.join(['?' for i in range(len(column_names))])))
        util.conn.executemany('INSERT INTO {} ({}) VALUES ({})'.format(table_name, ', '.join([i for i in column_names]), ', '.join(['?' for i in range(len(column_names))])), list_of_tuples)
        util.conn.commit()
        print('committed')


    def filter(self, table_name, req_value, **input_values):
        query = f'''SELECT DISTINCT {req_value} from {table_name}  WHERE'''
        for i in input_values.keys():
            query += f' {i} = ? AND'
        if query.endswith('AND'):
            query = query[:-4]
        self.cursor.execute(query, tuple(input_values.values()))
        return self.cursor.fetchall()

if __name__ == '__main__':
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket_name = 'noaa-goes18'
    table_name = bucket_name.replace('-', '_')
    paginator = s3.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket_name, Prefix='ABI-L1b-RadC')
    util = DbUtil('metadata1.db')
    try:
        # creation of table
        column_names = ['id INTEGER PRIMARY KEY', 'product TEXT', 'year TEXT', 'day_of_year TEXT', 'hour TEXT']
        util.create_table(table_name, *column_names)

        # insertion of rows
        rows = set()
        for page in pages:
            for i, obj in enumerate(page['Contents']):
                print('i', i)
                levels = obj['Key'].split('/')
                if(len(levels) == 5):
                    product = levels[0]
                    year = levels[1]
                    day_of_year = levels[2]
                    hour = levels[3]
                    rows.add((product, year, day_of_year, hour))
                if ((i+1) % 1000 == 0):
                    print('hi')
                    util.insert(table_name, ['product', 'year', 'day_of_year', 'hour'],rows)
                    rows = set()
        if rows:
            util.insert(table_name, ['product', 'year', 'day_of_year', 'hour'],rows)

        cursor = util.conn.cursor()
        cursor.execute(f"SELECT count(*) FROM {table_name}")
        result = cursor.fetchall()
        print(result)
        # print(util.filter('geos18', 'day_of_year', product = 'ABI-L1b-RadC', year = '2022'))
        # print(util.filter('geos18', 'year', product = 'ABI-L1b-RadC'))
        print(util.filter(table_name, 'hour', product = 'ABI-L1b-RadC', year = '2022', day_of_year = '209'))
    finally:
        util.conn.close()
