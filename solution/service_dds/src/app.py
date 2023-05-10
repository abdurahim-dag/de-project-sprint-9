import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader import DdsMessageProcessor, DdsRepository

app = Flask(__name__)

config = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()

    # Инициализируем процессор сообщений.
    # Пока он пустой. Нужен для того, чтобы потом в нем писать логику обработки сообщений из Kafka.
    proc = DdsMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        dds_repository=DdsRepository(db=config.pg_warehouse_db()),
        batch_size=100,
        logger=app.logger,
    )

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика(SampleMessageProcessor).
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # стартуем Flask-приложение.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
