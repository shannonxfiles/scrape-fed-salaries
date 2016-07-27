from jinja2 import Template
import psycopg2
import os
import json
import requests
import logging
logging.basicConfig(level=logging.INFO)

num_display = 100
TABLE_NAME = "federal_salaries"

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


def load_data(data):
    """
    load fed sal data to postgres
    """
    conn = get_postgres_conn()
    curs = conn.cursor()

    for record in data:

        try:
            curs.execute("INSERT INTO %s VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                         (TABLE_NAME, record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]))
        except psycopg2.IntegrityError as e:
            logging.error("Issue executing sql statement, integrity error")
            logging.error(e)
        except psycopg2.Error as e:
            logging.error("Issue executing sql statement")
            logging.error(e)

            if conn:
                conn.rollback()

    conn.commit()

    curs.close()
    conn.close()


def remove_erroneous_chars(data_to_clean):
    chars_to_remove = {'comma': ',', 'dollar': '$'}

    logging.debug("Data before cleaning: {}".format(data_to_clean))
    separate_change = data_to_clean.split('.')
    final_sal = separate_change[0].replace(',', '')
    final_sal = final_sal.replace('$', '')
    cleaned_data = int(final_sal.replace('.', ''))
    logging.debug("Data after cleaning: {}".format(cleaned_data))

    return cleaned_data


def clean_data(data):
    for i in data:
        # Grade
        i[1] = int(i[1])
        # Salary
        i[3] = remove_erroneous_chars(i[3])
        # Bonus
        i[4] = remove_erroneous_chars(i[4])
        # Year
        i[8] = int(i[8])

    return data


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
    next_iter = 0

    while paging_count > 0:
        raw_data = get_paged_table_data(next_iter)
        cleaned_data = clean_data(raw_data)
        load_data(cleaned_data)

        next_iter += 100
        paging_count -= 1