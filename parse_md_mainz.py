from dbfread import DBF
import psycopg2

connection = psycopg2.connect(user = "bastion",
							  password = "qwerty7gas",
							  host = "127.0.0.1",
							  port = "5432",
							  database = "astz")

for record in DBF('/home/bastion/MD_MAINZ-20190827/MD_mainz.DBF'):
    real_keys = []
    values = []
    for key in record.keys():
        val = record[key]
        if key == 'IS':
            key = 'IS_'
        real_keys.append(key)
        values.append(val)

    cursor = connection.cursor()
    cursor.execute("INSERT INTO tz ({}) VALUES ({});".format(','.join(real_keys), ','.join(map(lambda x: '%s', real_keys)) ), values )
    connection.commit()
    record = cursor.fetchone()
    break
