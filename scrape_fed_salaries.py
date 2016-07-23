import psycopg2
import os
import logging
logging.basicConfig(level=logging.INFO)

num_to_display = 100


def get_postgres_conn():
    # setup connection with env vars
    username = os.environ['PGUSER']
    password = os.environ['PGPASSWORD']
    database = os.environ['DB_NAME']
    hostname = os.environ['DB_HOST']
    return psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname
    )

#TODO: function to determine number of available display records
def get_max_display_record():
    never_hit_record_count = 1000000000
    # for display have some arbitrary ridiculous number
    url = """
        http://www.fedsdatacenter.com/federal-pay-rates/output.php?n=&a=&l=&o=&y=2015&s
            Echo=1&iColumns=9&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C&iDisplayStart={0}&i
            DisplayLength={1}&mDataProp_0=0&bSortable_0=true&mDataProp_1=1&bSortable_1
            =true&mDataProp_2=2&bSortable_2=true&mDataProp_3=3&bSortable_3=true&
            mDataProp_4=4&bSortable_4=true&mDataProp_5=5&bSortable_5=true&mDataProp_6=6&
            bSortable_6=true&mDataProp_7=7&bSortable_7=true&mDataProp_8=8&bSortable_8=true
            &iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&_=1469308589082
        """.format(never_hit_record_count, num_to_display)


def url_builder(nextIter):
    base_url = """
            http://www.fedsdatacenter.com/federal-pay-rates/output.php?n=&a=&l=&o=&y=2015&s
            Echo=1&iColumns=9&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C&iDisplayStart={0}&i
            DisplayLength={1}&mDataProp_0=0&bSortable_0=true&mDataProp_1=1&bSortable_1
            =true&mDataProp_2=2&bSortable_2=true&mDataProp_3=3&bSortable_3=true&
            mDataProp_4=4&bSortable_4=true&mDataProp_5=5&bSortable_5=true&mDataProp_6=6&
            bSortable_6=true&mDataProp_7=7&bSortable_7=true&mDataProp_8=8&bSortable_8=true
            &iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&_=1469308589082
            """.format(nextIter, num_to_display)


def main():
    exit(0)