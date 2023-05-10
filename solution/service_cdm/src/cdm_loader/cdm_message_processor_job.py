from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer

from .repository import CdmRepository
from .repository import UserCategoryCounters
from .repository import UserProductCounters


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger,
                 batch_size: int = 100,
                 ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START CDM Loader")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                break

            user_id = msg['user_id']
            products = msg['products']
            categories = msg['categories']
            user_product_counters = []
            user_category_counters = []

            # Заполним список моделей

            for product_id in products.keys():
                user_product_counters.append(
                    UserProductCounters(
                        user_id=user_id,
                        id=product_id,
                        name=products[product_id]['product_name'],
                        order_cnt=products[product_id]['order_cnt']
                    )
                )

            for category_id in categories.keys():
                user_category_counters.append(
                    UserCategoryCounters(
                        user_id=user_id,
                        id=category_id,
                        name=categories[category_id]['category_name'],
                        order_cnt=categories[category_id]['order_cnt']
                    )
                )

            # Для всех моделей из общего списка запускаем загрузку в слой.
            for model in user_product_counters + user_category_counters:
                # Параметры, для запроса
                params = model.dict(by_alias=True, exclude={'table', 'on_conflict'})
                self._cdm_repository.insert(
                    table_name=model.table,
                    params=params,
                    on_conflict=model.on_conflict # список столбцов на проверку уникальности записей
                )

            self._logger.info(f"CDM Models saved.")

        self._logger.info(f"{datetime.utcnow()}: FINISH CDM Loader")
