from jinja2 import Template
import psycopg2
import os
import time
import json
import requests
from requests.exceptions import ConnectionError
import logging
import logging.handlers
logging.basicConfig(level=logging.DEBUG)
LOG_FILENAME = "log/fed_salary_log.log"

num_display = 500
TABLE_NAME = "fed.federal_salaries"
YEAR = 2016

base_url = Template(
                    'http://www.fedsdatacenter.com/federal-pay-rates/output.php?n=&a=&l=&o=&y={{year}}&s'
                    'Echo=1&iColumns=9&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C&iDisplayStart={{next_record_start}}&i'
                    'DisplayLength={{num_display}}&mDataProp_0=0&bSortable_0=true&mDataProp_1=1&bSortable_1'
                    '=true&mDataProp_2=2&bSortable_2=true&mDataProp_3=3&bSortable_3=true&'
                    'mDataProp_4=4&bSortable_4=true&mDataProp_5=5&bSortable_5=true&mDataProp_6=6&'
                    'bSortable_6=true&mDataProp_7=7&bSortable_7=true&mDataProp_8=8&bSortable_8=true'
                    '&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&_=1469308589082'
                    )


def setup_logger(file_log=False):
    if file_log is True:
        l = logging.getLogger()
        rotate_file_handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=1000000, backupCount=100)

        f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
        rotate_file_handler.setFormatter(f)
        l.addHandler(rotate_file_handler)


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
            curs.execute("INSERT INTO fed.federal_salaries VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                         (record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]))
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
    logging.debug("All the data: {}".format(data))
    for i in data:
        logging.debug("Current record: {}".format(i))
        # Grade
        # TODO: More granular grade (some strings some numbers, separate)
        # i[1] = int(i[1])
        # logging.debug("Grade: {}". format(i[1]))

        # Salary
        i[3] = remove_erroneous_chars(i[3])
        logging.debug("Salary: {}". format(i[3]))
        # Bonus
        i[4] = remove_erroneous_chars(i[4])
        logging.debug("Bonus: {}". format(i[4]))
        # Year
        i[8] = int(i[8])
        logging.debug("Year: {}". format(i[8]))

    return data


def get_max_display_record():
    never_hit_record_count = 1000000000

    # for display have some arbitrary ridiculous number
    url = base_url.render(next_record_start=never_hit_record_count, num_display=num_display, year=YEAR)
    headers = {'user-agent': 'python personal project app/0.0.1'}
    generated_url = requests.get(url, headers=headers)
    get_data = json.loads(generated_url.text)
    get_max_record_count = get_data['iTotalDisplayRecords']

    return get_max_record_count


def get_paged_table_data(next_iter):
    url = base_url.render(next_record_start=next_iter, num_display=num_display, year=YEAR)
    try:
        generated_url = requests.get(url)
    except ConnectionError as e:
        logging.error(e)
        time.sleep(300)
        logging.error("Trying connecting again after sleep")
        generated_url = requests.get(url)

    get_data = json.loads(generated_url.text)
    data = get_data['aaData']
    return data


def main():
    setup_logger(file_log=True)
    paging_count = int(get_max_display_record()) / num_display
    next_iter = 0

    while paging_count > 0:
        logging.info("Start processing data with paging count '{}'".format(paging_count))
        raw_data = get_paged_table_data(next_iter)
        logging.info("Paged records successfully pulled")
        cleaned_data = clean_data(raw_data)
        logging.info("Data cleaned!")
        load_data(cleaned_data)
        logging.info("Data with paging count '{}' finished processing!".format(paging_count))

        next_iter += num_display
        logging.debug("Next iteration start number: {}".format(next_iter))
        paging_count -= 1
        # TODO: Add retry
        time.sleep(30)


if __name__ == "__main__":
    main()