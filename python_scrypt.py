"""
2.Python, Docker
    Создать git проект, в котором должно быть 2 docker контейнера:
        скрипт python;
        БД (postgreSQL).

    Алгоритм взаимодействия.
        Скрипт каждую минуту отправляет данные в БД cо сгенерированными данными.
        Пример данных:
            "id": id записи (инкремент);
            "data": сгенерированная строка данных;
            "date": текущая дата и время.

    Скрипт логирует свои действия.
    При достижении в таблице БД 30 строк, таблица должна очищаться и вновь пришедшие
    данные должны быть записаны 1й строчкой. (Можно реализовать на уровне БД или на
    уровне скрипта)
    Проект разворачивается с помощью docker compose.
"""

import logging
import os
import random
from datetime import datetime, timezone
from time import sleep

import psycopg2


DB_HOST = os.getenv('POSTGRES_HOST')
DB_NAME = os.getenv('POSTGRES_USER')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
MAX_LENGTH_STRING = 30

logging.basicConfig(filename='logs.log', level=logging.INFO)


def get_random_string() -> str:
    return ''.join(
        random.choice('abcdefghijklmnopqrstuvwxyz0123456789')
        for _ in range(1, MAX_LENGTH_STRING)
    )


def get_connectoion() -> 'psycopg2.connection':
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info('Connected to db success.')
        return conn
    except:
        logging.error('Connected to db failed.')
        exit()

conn = get_connectoion()
cur = conn.cursor()

cur.execute(
    f"""
    CREATE TABLE IF NOT EXISTS some_table (
    id SERIAL PRIMARY KEY,
    data VARCHAR({MAX_LENGTH_STRING}),
    date TIMESTAMP
    );
    """
)

conn.commit()

while True:
    data = get_random_string()
    date = datetime.now(timezone.utc)
    cur.execute(
        f"""
        INSERT INTO some_table (data, date)
        VALUES ('{data}', '{date}')
        RETURNING id
        """
    )
    conn.commit()
    id_=cur.fetchone()[0]

    logging.info(
        f'Add row: id={id_}, data={data}, date={date}'
    )
    cur.execute('SELECT COUNT(*) FROM some_table')
    count = cur.fetchone()[0]
    logging.info(f'count rows={count}')

    if count >= 30:
        cur.execute('DELETE FROM some_table')
        conn.commit()
        logging.info('Delete all rows.')

    sleep(60)
