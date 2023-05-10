from typing import Any
import hashlib
import uuid
from datetime import datetime
from logging import Logger
from collections import defaultdict
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from .repository import (
    DdsRepository,
    LProductCategory, LProductRestaurant, LOrderProduct, LOrderUser,
    SProductName, SRestaurantName, SUserName, SOrderCost, SOrderStatus,
    HUser, HOrder, HProduct, HCategory, HRestaurant

)


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger,
                 batch_size: int = 100,
                 ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START DDS Loader")        

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                break

            # Вычислим необходимые новые поля.
            msg['load_dt'] = datetime.utcnow()
            msg['h_order_pk'] = self._gen_uuid(str(msg['object_id']))
            msg['payload']['user']['h_user_pk'] = self._gen_uuid(msg['payload']['user']['id'])
            msg['payload']['restaurant']['h_restaurant_pk'] = self._gen_uuid(msg['payload']['restaurant']['id'])
            # И тут же подсчитаем количества продуктов и категорий.
            products = dict()
            categories = dict()
            cnt = defaultdict(int)
            for product in msg['payload']['products']:
                h_category_pk = self._gen_uuid(product['category'])
                h_product_pk = self._gen_uuid(product['id'])

                product['h_product_pk'] = h_product_pk
                product['h_category_pk'] = h_category_pk

                products[str(h_product_pk)] = {
                    'product_name': product['name'],
                    'order_cnt': product['quantity'],
                }

                cnt[h_category_pk] += 1
                categories[str(h_category_pk)] = {
                    'category_name': product['category'],
                    'order_cnt': cnt[h_category_pk],
                }

            # Генерим на основе входного сообщения модели хабов, сателлитов и линков.
            hubs = self._get_hubs(msg)
            satellites = self._get_satellites(msg)
            links = self._get_links(msg)

            # Заганяем всё сразу в слой данных.
            for model in hubs + satellites + links:
                table_name = model.table
                model_dict = model.dict()
                del model_dict['table']
                self._dds_repository.insert(
                    table_name=table_name,
                    params=model_dict
                )

            # Генерим сообщение, для следующего сервиса.
            result = {
                'user_id': msg['payload']['user']['h_user_pk'],
                'categories': categories,
                'products': products
            }

            self._producer.produce(result)
            self._logger.info(f"{datetime.utcnow()}: SENT PRODUCER")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH DDS Loader")

    @classmethod
    def _gen_uuid(cls, value: str) -> uuid.UUID:
        """Генератор UUID(sha-1) по заданной строке."""
        return uuid.uuid5(uuid.NAMESPACE_OID, value)

    @classmethod
    def _gen_hashdiff(cls, row: dict) -> uuid.UUID:
        """Генератор UUID хэш-ключа, для определения уникальности загруженной строки."""
        # Сериализируем данные в строку.
        row_string = str(row)

        # Вычисляем хэш SHA-256
        hash_object = hashlib.sha256(row_string.encode('utf-8'))
        hash_hex = hash_object.hexdigest()[::2]

        return uuid.UUID(hash_hex)

    def _get_hubs(self, msg:  dict) -> [Any]:
        """Функция возвращает хабы из сообщения."""
        result = []
        # Для всех продуктов генерим хабы h_product и h_category и заносим в общий список.
        for product in msg['payload']['products']:
            result.append(
                HProduct(
                    load_dt=msg['load_dt'],
                    product_id=product['id'],
                    h_product_pk=product['h_product_pk']
                )
            )
            result.append(
                HCategory(
                    load_dt=msg['load_dt'],
                    category_name=product['category'],
                    h_category_pk=product['h_category_pk']
                )
            )
        # Добавляем в общий список оставшиеся хабы.
        result.extend(
            [
                HOrder(
                    load_dt=msg['load_dt'],
                    order_id=msg['object_id'],
                    order_dt= datetime.strptime(msg['payload']['date'], '%Y-%m-%d %H:%M:%S'),
                    h_order_pk=msg['h_order_pk']
                ),
                HUser(
                    load_dt=msg['load_dt'],
                    user_id=msg['payload']['user']['id'],
                    h_user_pk=msg['payload']['user']['h_user_pk']
                ),
                HRestaurant(
                    load_dt=msg['load_dt'],
                    restaurant_id=msg['payload']['restaurant']['id'],
                    h_restaurant_pk=msg['payload']['restaurant']['h_restaurant_pk']
                )
            ]
        )
        return result


    def _get_satellites(self, msg:  dict) -> [Any]:
        """Функция возвращает сателлиты из сообщения."""
        user = msg['payload']['user']
        order = {
            'id': msg['object_id'],
            'cost': msg['payload']['cost'],
            'payment': msg['payload']['payment'],
            'status': msg['payload']['status'],
            'h_order_pk': msg['h_order_pk']
        }

        result = [
            SOrderCost(
                h_order_pk=order['h_order_pk'],
                load_dt=msg['load_dt'],
                cost=order['cost'],
                payment=order['payment']
            ),
            SOrderStatus(
                h_order_pk=order['h_order_pk'],
                load_dt=msg['load_dt'],
                status=order['status']
            ),
            SUserName(
                h_user_pk=user['h_user_pk'],
                load_dt=msg['load_dt'],
                username=user['name'],
            ),
            SRestaurantName(
                load_dt=msg['load_dt'],
                name=msg['payload']['restaurant']['name'],
                h_restaurant_pk=msg['payload']['restaurant']['h_restaurant_pk']
            )
        ]

        for product in msg['payload']['products']:
            result.append(
                SProductName(
                    load_dt=msg['load_dt'],
                    name=product['name'],
                    h_product_pk=product['h_product_pk']
                )
            )

        # У всех моделей поле *_hashdiff заполняется одинаково.
        for model in result:
            for field_name in model.__fields__:
                if 'hashdiff' in field_name:
                    setattr(model, field_name, self._gen_hashdiff(model.dict()))

        return result

    def _get_links(self, msg: dict) -> [Any]:
        """Функция возвращает линки из сообщения."""
        result = [
            LOrderUser(
                h_order_pk=msg['h_order_pk'],
                h_user_pk=msg['payload']['user']['h_user_pk'],
                hk_order_user_pk=uuid.uuid4(),
                load_dt=msg['load_dt']
            ),
        ]

        for product in msg['payload']['products']:
            result.extend(
                [
                    LOrderProduct(
                        h_order_pk=msg['h_order_pk'],
                        h_product_pk=product['h_product_pk'],
                        hk_order_product_pk=uuid.uuid4(),
                        load_dt=msg['load_dt']
                    ),
                    LProductRestaurant(
                        h_restaurant_pk=msg['payload']['restaurant']['h_restaurant_pk'],
                        h_product_pk=product['h_product_pk'],
                        hk_product_restaurant_pk=uuid.uuid4(),
                        load_dt=msg['load_dt']
                    ),
                    LProductCategory(
                        h_product_pk=product['h_product_pk'],
                        h_category_pk=product['h_category_pk'],
                        hk_product_category_pk=uuid.uuid4(),
                        load_dt=msg['load_dt']
                    )
                ]
            )

        return result