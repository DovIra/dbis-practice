import psycopg2
import datetime
import logging
import csv
import psycopg2.errorcodes
from config import db

def restore_db_connection():
    global conn, cursor
    try:
        conn = psycopg2.connect(**db)
        cursor = conn.cursor()
    except psycopg2.OperationalError as e:
        is_connected = False
        while not is_connected:
            try:
                conn = psycopg2.connect(**db)
                cursor = conn.cursor()
                print("Connection restored!")
                logger.info('Connection restored!')
                is_connected = True
            except psycopg2.OperationalError as e:
                pass

def create_table():
    cursor.execute('DROP TABLE IF EXISTS zno_results;')
    conn.commit()

    query = '''
            CREATE TABLE IF NOT EXISTS zno_results (
                YEAR INT NOT NULL,
                OUTID VARCHAR(255) PRIMARY KEY,
                Birth NUMERIC NOT NULL,
                SEXTYPENAME VARCHAR(255) NOT NULL,
                REGNAME VARCHAR(255) NOT NULL,
                AREANAME VARCHAR(255) NOT NULL,
                TERNAME VARCHAR(255) NOT NULL,
                REGTYPENAME VARCHAR(255) NOT NULL,
                TerTypeName VARCHAR(255) NOT NULL,
                ClassProfileNAME VARCHAR(255) DEFAULT NULL,
                ClassLangName VARCHAR(255) DEFAULT NULL,
                EONAME VARCHAR(255) DEFAULT NULL,
                EOTYPENAME VARCHAR(255) DEFAULT NULL,
                EORegName VARCHAR(255) DEFAULT NULL,
                EOAreaName VARCHAR(255) DEFAULT NULL,
                EOTerName VARCHAR(255) DEFAULT NULL,
                EOParent VARCHAR(255) DEFAULT NULL,
                UkrTest VARCHAR(255) DEFAULT NULL,
                UkrTestStatus VARCHAR(255) DEFAULT NULL,
                UkrBall100 NUMERIC DEFAULT NULL,
                UkrBall12 NUMERIC DEFAULT NULL,
                UkrBall NUMERIC DEFAULT NULL,
                UkrAdaptScale NUMERIC DEFAULT NULL,
                UkrPTName VARCHAR(255) DEFAULT NULL,
                UkrPTRegName VARCHAR(255) DEFAULT NULL,
                UkrPTAreaName VARCHAR(255) DEFAULT NULL,
                UkrPTTerName VARCHAR(255) DEFAULT NULL,
                histTest VARCHAR(255) DEFAULT NULL,
                HistLang VARCHAR(255) DEFAULT NULL,
                histTestStatus VARCHAR(255) DEFAULT NULL,
                histBall100 NUMERIC DEFAULT NULL,
                histBall12 NUMERIC DEFAULT NULL,
                histBall NUMERIC DEFAULT NULL,
                histPTName VARCHAR(255) DEFAULT NULL,
                histPTRegName VARCHAR(255) DEFAULT NULL,
                histPTAreaName VARCHAR(255) DEFAULT NULL,
                histPTTerName VARCHAR(255) DEFAULT NULL,
                mathTest VARCHAR(255) DEFAULT NULL,
                mathLang VARCHAR(255) DEFAULT NULL,
                mathTestStatus VARCHAR(255) DEFAULT NULL,
                mathBall100 NUMERIC DEFAULT NULL,
                mathBall12 NUMERIC DEFAULT NULL,
                mathBall NUMERIC DEFAULT NULL,
                mathPTName VARCHAR(255) DEFAULT NULL,
                mathPTRegName VARCHAR(255) DEFAULT NULL,
                mathPTAreaName VARCHAR(255) DEFAULT NULL,
                mathPTTerName VARCHAR(255) DEFAULT NULL,
                physTest VARCHAR(255) DEFAULT NULL,
                physLang VARCHAR(255) DEFAULT NULL,
                physTestStatus VARCHAR(255) DEFAULT NULL,
                physBall100 NUMERIC DEFAULT NULL,
                physBall12 NUMERIC DEFAULT NULL,
                physBall NUMERIC DEFAULT NULL,
                physPTName VARCHAR(255) DEFAULT NULL,
                physPTRegName VARCHAR(255) DEFAULT NULL,
                physPTAreaName VARCHAR(255) DEFAULT NULL,
                physPTTerName VARCHAR(255) DEFAULT NULL,
                chemTest VARCHAR(255) DEFAULT NULL,
                chemLang VARCHAR(255) DEFAULT NULL,
                chemTestStatus VARCHAR(255) DEFAULT NULL,
                chemBall100 NUMERIC DEFAULT NULL,
                chemBall12 NUMERIC DEFAULT NULL,
                chemBall NUMERIC DEFAULT NULL,
                chemPTName VARCHAR(255) DEFAULT NULL,
                chemPTRegName VARCHAR(255) DEFAULT NULL,
                chemPTAreaName VARCHAR(255) DEFAULT NULL,
                chemPTTerName VARCHAR(255) DEFAULT NULL,
                bioTest VARCHAR(255) DEFAULT NULL,
                bioLang VARCHAR(255) DEFAULT NULL,
                bioTestStatus VARCHAR(255) DEFAULT NULL,
                bioBall100 NUMERIC DEFAULT NULL,
                bioBall12 NUMERIC DEFAULT NULL,
                bioBall NUMERIC DEFAULT NULL,
                bioPTName VARCHAR(255) DEFAULT NULL,
                bioPTRegName VARCHAR(255) DEFAULT NULL,
                bioPTAreaName VARCHAR(255) DEFAULT NULL,
                bioPTTerName VARCHAR(255) DEFAULT NULL,
                geoTest VARCHAR(255) DEFAULT NULL,
                geoLang VARCHAR(255) DEFAULT NULL,
                geoTestStatus VARCHAR(255) DEFAULT NULL,
                geoBall100 NUMERIC DEFAULT NULL,
                geoBall12 NUMERIC DEFAULT NULL,
                geoBall NUMERIC DEFAULT NULL,
                geoPTName VARCHAR(255) DEFAULT NULL,
                geoPTRegName VARCHAR(255) DEFAULT NULL,
                geoPTAreaName VARCHAR(255) DEFAULT NULL,
                geoPTTerName VARCHAR(255) DEFAULT NULL,
                engTest VARCHAR(255) DEFAULT NULL,
                engTestStatus VARCHAR(255) DEFAULT NULL,
                engBall100 NUMERIC DEFAULT NULL,
                engBall12 NUMERIC DEFAULT NULL,
                engDPALevel VARCHAR(255) DEFAULT NULL,
                engBall NUMERIC DEFAULT NULL,
                engPTName VARCHAR(255) DEFAULT NULL,
                engPTRegName VARCHAR(255) DEFAULT NULL,
                engPTAreaName VARCHAR(255) DEFAULT NULL,
                engPTTerName VARCHAR(255) DEFAULT NULL,
                fraTest VARCHAR(255) DEFAULT NULL,
                fraTestStatus VARCHAR(255) DEFAULT NULL,
                fraBall100 NUMERIC DEFAULT NULL,
                fraBall12 NUMERIC DEFAULT NULL,
                fraDPALevel VARCHAR(255) DEFAULT NULL,
                fraBall NUMERIC DEFAULT NULL,
                fraPTName VARCHAR(255) DEFAULT NULL,
                fraPTRegName VARCHAR(255) DEFAULT NULL,
                fraPTAreaName VARCHAR(255) DEFAULT NULL,
                fraPTTerName VARCHAR(255) DEFAULT NULL,
                deuTest VARCHAR(255) DEFAULT NULL,
                deuTestStatus VARCHAR(255) DEFAULT NULL,
                deuBall100 NUMERIC DEFAULT NULL,
                deuBall12 NUMERIC DEFAULT NULL,
                deuDPALevel VARCHAR(255) DEFAULT NULL,
                deuBall NUMERIC DEFAULT NULL,
                deuPTName VARCHAR(255) DEFAULT NULL,
                deuPTRegName VARCHAR(255) DEFAULT NULL,
                deuPTAreaName VARCHAR(255) DEFAULT NULL,
                deuPTTerName VARCHAR(255) DEFAULT NULL,
                spaTest VARCHAR(255) DEFAULT NULL,
                spaTestStatus VARCHAR(255) DEFAULT NULL,
                spaBall100 NUMERIC DEFAULT NULL,
                spaBall12 NUMERIC DEFAULT NULL,
                spaDPALevel VARCHAR(255) DEFAULT NULL,
                spaBall NUMERIC DEFAULT NULL,
                spaPTName VARCHAR(255) DEFAULT NULL,
                spaPTRegName VARCHAR(255) DEFAULT NULL,
                spaPTAreaName VARCHAR(255) DEFAULT NULL,
                spaPTTerName VARCHAR(255) DEFAULT NULL
            )
        '''

    cursor.execute(query)
    conn.commit()
    logger.info('Table is created')

def get_csv_header(file_csv):
    with open(file_csv, "r", encoding='cp1251', newline='') as csv_file:
        reader = csv.reader(csv_file)
        csv_row = next(reader)
        header_fields = csv_row[0].split(';')
        return header_fields

def insert_from_csv(file_csv, year):
    global conn, cursor
    start_time = datetime.datetime.now()
    logger.info("Start process " + file_csv + " data file")
    print ("Start process " + file_csv + " data file")

    headers = get_csv_header( file_csv )
    #print (headers)
    with open(file_csv, "r", encoding='cp1251') as csv_file_handler:
        csv_reader = csv.DictReader(csv_file_handler, delimiter=';')
        count = 0
        batch_number = 1
        batch_rows_count = 1000
        insert_query_header = '''INSERT INTO zno_results (year, ''' + ', '.join(headers).lower() + ') VALUES '
        insert_query = insert_query_header

        while True:
            try:
                row = next(csv_reader)
                count += 1
                for field_name in row:
                    if row[field_name] == 'null':
                        pass
                    elif field_name.lower() != 'birth' and 'ball' not in field_name.lower():
                        row[field_name] = "'" + row[field_name].replace("'", "''") + "'"
                    elif 'ball100' in field_name.lower():
                        row[field_name] = row[field_name].replace(',', '.')

                insert_query += '(' + str(year) + ", " + ",".join(row.values()) + ")"

                if count >= batch_rows_count:
                    try:
                        cursor.execute(insert_query)
                        conn.commit()
                        #print(str(batch_number) + ' batch inserted')

                        insert_query = insert_query_header
                        count = 0
                        batch_number += 1
                    except psycopg2.OperationalError as e:
                        logger.error('Connection lost...')
                        logger.error(e)
                        print('Connection lost...')
                        restore_db_connection()
                        insert_query += ','
                else:
                    insert_query += ','
            except csv.Error as e:
                print("line: {}, error: {}".format(csv_reader.line_num, e))
            except StopIteration:
                break

        if count > 0 and count < batch_rows_count:
            insert_query = insert_query.rstrip(',')
            try:
                cursor.execute(insert_query)
                conn.commit()
                #print(str(batch_number) + ' batch inserted')
            except psycopg2.OperationalError as e:
                print('Connection lost...')
                logger.error('Connection lost...')
                logger.error(e)
                restore_db_connection()

                cursor.execute(insert_query)
                conn.commit()
                #print(str(batch_number) + ' batch inserted')

        print("Data import completed!")

    end_time = datetime.datetime.now()
    logger.info("File process is completed")
    logger.info('Process time: ' + str(end_time - start_time) + '\n')

def set_zno_results():
    # найкращий бал з Англійської мови у 2020 та 2019 роках
    # серед тих кому було зараховано тест
    query = '''
    SELECT 
        year,
        max(engBall100) as max_ball_100,
        max(engBall12) as max_ball_12,
        max(engBall) as max_ball
    from zno_results
    where engTestStatus = 'Зараховано'
    group by year
    order by year asc
'''
    cursor.execute(query)

    with open('result.csv', 'w', encoding="utf-8", newline='') as result_csv_file:
        csv_writer = csv.writer(result_csv_file, delimiter=';')
        csv_writer.writerow(["Year", "Test Value 200", "Test Value 12", "Test Value"])
        for row in cursor:
            csv_writer.writerow(row)
    logger.info("ZNO results were saved")
    print ("ZNO results were saved")

def log_best_zno_result():
    # Порівняти найкращий бал з Англійської мови у 2020 та 2019 роках
    # серед тих кому було зараховано тест
    query = '''
    WITH some_count AS (
       SELECT 
            year,
            max(engBall100) as max_ball_100,
            max(engBall12) as max_ball_12,
            max(engBall) as max_ball
        from zno_results
        where engTest = 'Англійська мова' and engTestStatus = 'Зараховано'
        group by year
    )
    SELECT
          *
    FROM some_count bb
    order by max_ball desc
    limit 1
'''
    cursor.execute(query)
    for row in cursor:
        logger.info("The best result was in " + str(row[0]) + " year with " + str(row[3]) + " test value")
        print ("The best result was in " + str(row[0]) + " year with " + str(row[3]) + " test value")

# ----------------------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(filename='results.log', level=logging.INFO,
                    format='%(asctime)s | %(levelname)s | %(message)s')

try:
    conn = psycopg2.connect(**db)
    cursor = conn.cursor()
    logger.info('Connection established')
except psycopg2.OperationalError as e:
    print('Connection is not established')
    logger.error('Connection is not established')
    logger.error(e)

    is_connected = False
    while not is_connected:
        try:
            conn = psycopg2.connect(**db)
            cursor = conn.cursor()
            print("Connection restored!")
            logger.info('Connection restored!')
            is_connected = True
        except psycopg2.OperationalError as e:
            pass

create_table()

insert_from_csv("Odata2019File.csv", 2019)
insert_from_csv("Odata2020File.csv", 2020)

set_zno_results()
log_best_zno_result()

cursor.close()
conn.close()