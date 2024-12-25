import os
import psycopg2
import datetime
import boto3
from botocore.client import Config
import uuid
import logging
from psycopg2.extras import execute_values

def handler(event, context):
    # Настройка логирования
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        logger.info("Получено событие: %s", event)

        # Извлечение сообщений из события
        messages = event.get('messages', [])

        if not messages:
            logger.info("Ключ 'messages' отсутствует или пуст.")
            return {'status': 'no_records'}

        # Инициализация S3 клиента с использованием Yandex.Cloud S3-совместимого API
        s3_client = boto3.client(
            's3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],        # Ваш Access Key
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],# Ваш Secret Key
            config=Config(signature_version='s3v4'),
            region_name='ru-central1'  # Замените на ваш регион, если необходимо
        )

        # Инициализация соединения с базой данных один раз
        try:
            conn = psycopg2.connect(
                dbname=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                host=os.environ['DB_HOST'],
                port=os.environ['DB_PORT']
            )
            cursor = conn.cursor()
            logger.info("Соединение с базой данных установлено.")
        except Exception as e:
            logger.error("Ошибка при подключении к базе данных: %s", e)
            return {'status': 'error', 'message': str(e)}

        for message in messages:
            details = message.get('details', {})
            bucket_id = details.get('bucket_id')
            object_id = details.get('object_id')

            if not bucket_id or not object_id:
                logger.warning("Пропущен message без 'bucket_id' или 'object_id': %s", message)
                continue

            logger.info("Обработка файла: s3://%s/%s", bucket_id, object_id)

            # Уникальное имя файла
            unique_id = uuid.uuid4()
            sales_file = f'/tmp/sales_data_{unique_id}.txt'

            # Скачивание файла в /tmp/
            try:
                s3_client.download_file(bucket_id, object_id, sales_file)
                logger.info("Файл успешно скачан в %s.", sales_file)
            except Exception as e:
                logger.error("Ошибка при скачивании файла s3://%s/%s: %s", bucket_id, object_id, e)
                continue

            # Чтение и парсинг данных из файла
            sales_data = []
            try:
                with open(sales_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                logger.info("Файл прочитан. Количество строк: %d.", len(lines))

                for line_number, line in enumerate(lines, start=1):
                    parts = line.strip().split()
                    if len(parts) != 4:
                        logger.warning("Неверный формат строки %d: %s", line_number, line.strip())
                        continue
                    try:
                        sale_date = datetime.datetime.strptime(parts[0], '%Y-%m-%d').date()
                        product = parts[1]
                        quantity = int(parts[2])
                        cost = float(parts[3])
                        sales_data.append((sale_date, product, quantity, cost))
                        logger.info("Строка %d успешно обработана.", line_number)
                    except ValueError as ve:
                        logger.error("Ошибка при обработке строки %d: %s", line_number, ve)
                        continue
            except Exception as e:
                logger.error("Ошибка при чтении файла %s: %s", sales_file, e)
                continue

            if not sales_data:
                logger.info("Нет валидных данных для вставки.")
                continue

            # Вставка данных в базу данных
            insert_query = """
                INSERT INTO sales_records (sale_date, product, quantity, cost)
                VALUES %s
            """
            try:
                execute_values(cursor, insert_query, sales_data)
                conn.commit()
                logger.info("Успешно вставлено %d записей в базу данных.", len(sales_data))
            except Exception as e:
                conn.rollback()
                logger.error("Ошибка при вставке данных в базу данных: %s", e)
                continue

        cursor.close()
        conn.close()
        logger.info("Завершение обработки события.")
        return {'status': 'success'}

    except Exception as main_e:
        logger.exception("Необработанная ошибка: %s", main_e)
        return {'status': 'error', 'message': str(main_e)}
