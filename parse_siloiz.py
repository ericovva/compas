from dbfread import DBF
import psycopg2

connection = psycopg2.connect(user = "bastion",
                              password = "qwerty7gas",
                              host = "127.0.0.1",
                              port = "5432",
                              database = "astz")

all_cnt = 0
all_rows = 0
errors = []

for record in DBF('/home/bastion/Siloiz-20190827/siloiz.dbf'):
    all_rows+=1
    real_keys = []
    values = []
    for key in record.keys():
        val = record[key]
        real_keys.append(key)
        values.append(val)

    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO siloiz ({}) VALUES ({});".format(','.join(real_keys), ','.join(map(lambda x: '%s', real_keys)) ), values)
        cnt = cursor.rowcount
        all_cnt+=cnt
    except Exception as e:
        print(e)
        print(record)
        errors.append(e)
        connection.rollback()
        continue
    if all_cnt % 100 == 0:
        print("insert {} from {}".format(all_cnt, all_rows))
        connection.commit()

connection.commit()
print('ALL DONE')
print(errors)
connection.close()
