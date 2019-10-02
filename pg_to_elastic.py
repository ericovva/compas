import logging
import time

import psycopg2
from elasticsearch import Elasticsearch


def connect_elasticsearch():
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}], http_auth=('elastic', 'qwerty7gas'))
    if _es.ping():
        print('Connected')
        return _es
    else:
        print('Could not connect!')
        return None


def create_index(es_object, index_name='trademarks'):
    created = False
    settings = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "my_analyzer": {
                            "type": "custom",
                            "tokenizer": "whitespace",
                            "filter": [
                                "uppercase",
                                "my_metaphone"
                            ]
                        }
                    },
                    "filter": {
                        "my_metaphone": {
                            "type": "phonetic",
                            "encoder": "metaphone"
                        }
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "title": {
                    "type": "text",
                    "fields": {
                        "phonetic": {
                            "type": "text",
                            "analyzer": "my_analyzer"
                        },
                    }
                },
                "USE": {
                    "type": "dense_vector",
                    "dims": 512
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(es_object, index_name, doc_id, record):
    try:
        outcome = es_object.index(index=index_name, id=doc_id, body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))


if __name__ == '__main__':
    start = time.perf_counter()
    logging.basicConfig(level=logging.INFO)

    itersize = 1024
    es = connect_elasticsearch()
    if not es:
        exit(-1)
    create_index(es, 'trademarks')
    connection = psycopg2.connect(user="bastion",
                                  password="qwerty7gas",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="astz")
    query = "select id, wwt from tz;"
    cursor = connection.cursor()
    cursor.itersize = itersize
    cursor.execute(query)
    counter = 0
    for record in cursor:
        if record[1]:
            store_record(es, 'trademarks', record[0], {
                'title': record[1]
            })
        counter += 1
        if not counter % itersize:
            print(f"stored {counter} records")

    stop = time.perf_counter()
    print(stop - start)

