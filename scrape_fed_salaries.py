from jinja2 import Template
import psycopg2
import os
import json
import requests
import logging
logging.basicConfig(level=logging.INFO)

num_display = 100

base_url = Template(
                    'http://www.fedsdatacenter.com/federal-pay-rates/output.php?n=&a=&l=&o=&y=2015&s'
                    'Echo=1&iColumns=9&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C&iDisplayStart={{next_record_start}}&i'
                    'DisplayLength={{num_display}}&mDataProp_0=0&bSortable_0=true&mDataProp_1=1&bSortable_1'
                    '=true&mDataProp_2=2&bSortable_2=true&mDataProp_3=3&bSortable_3=true&'
                    'mDataProp_4=4&bSortable_4=true&mDataProp_5=5&bSortable_5=true&mDataProp_6=6&'
                    'bSortable_6=true&mDataProp_7=7&bSortable_7=true&mDataProp_8=8&bSortable_8=true'
                    '&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&_=1469308589082'
                    )


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


def remove_erroneous_chars(data_to_clean):
    chars_to_remove = {'comma': ',', 'dollar': '$'}

    print data_to_clean
    separate_change = data_to_clean.split('.')
    final_sal = separate_change[0].replace(',', '')
    final_sal = final_sal.replace('$', '')
    cleaned_data = int(final_sal.replace('.', ''))
    print cleaned_data

    return cleaned_data


def clean_data(data):
    for i in data:
        print i[3]
        i[3] = remove_erroneous_chars(remove_erroneous_chars)
        print i[3]

        print i[4]
        i[4] = remove_erroneous_chars(remove_erroneous_chars)
        print i[4]

        i[8] = int(i[8])


def get_max_display_record():
    never_hit_record_count = 1000000000

    # for display have some arbitrary ridiculous number
    url = base_url.render(next_record_start=never_hit_record_count, num_display=num_display)

    generated_url = requests.get(url)
    get_data = json.loads(generated_url.text)
    get_max_record_count = get_data['iTotalDisplayRecords']

    return get_max_record_count


def get_paged_table_data(next_iter):
    url = base_url.render(next_record_start=next_iter, num_display=num_display)
    generated_url = requests.get(url)
    get_data = json.loads(generated_url.text)
    return get_data


def main():
    paging_count = int(get_max_display_record()) / num_display

    while paging_count > 0:


        paging_count -= 1