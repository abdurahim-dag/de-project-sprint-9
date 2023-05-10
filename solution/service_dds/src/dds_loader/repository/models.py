import datetime
import decimal
import uuid
from typing import List

from pydantic import (
    BaseModel,
    Field,
    validator
)
from typing import Dict
from uuid import UUID


class Base(BaseModel):
    load_dt: datetime.datetime
    load_src: str = Field('orders-system-kafka')


class HOrder(Base):
    table: str = 'h_order'
    order_id: int
    order_dt: datetime.datetime
    h_order_pk: UUID


class HUser(Base):
    table: str = 'h_user'
    h_user_pk: UUID
    user_id: str


class HProduct(Base):
    table: str = 'h_product'
    h_product_pk: UUID
    product_id: str


class HRestaurant(Base):
    table: str = 'h_restaurant'
    h_restaurant_pk: UUID
    restaurant_id: str


class HCategory(Base):
    table: str = 'h_category'
    h_category_pk: UUID
    category_name: str


class SOrderCost(Base):
    table: str = 's_order_cost'
    cost: decimal.Decimal
    payment: decimal.Decimal
    h_order_pk: UUID
    hk_order_cost_hashdiff: UUID | None


class SOrderStatus(Base):
    table: str = 's_order_status'
    h_order_pk: UUID
    status: str
    hk_order_status_hashdiff: UUID | None


class SUserName(Base):
    table: str = 's_user_names'
    h_user_pk: UUID
    username: str
    hk_user_names_hashdiff: UUID | None
    userlogin: str = ''


class SProductName(Base):
    table: str = 's_product_names'
    h_product_pk: UUID
    name: str
    hk_product_names_hashdiff: UUID | None


class SRestaurantName(Base):
    table: str = 's_restaurant_names'
    h_restaurant_pk: UUID
    name: str
    hk_restaurant_names_hashdiff: UUID | None


class LOrderUser(Base):
    table: str = 'l_order_user'
    hk_order_user_pk: UUID
    h_order_pk: UUID
    h_user_pk: UUID


class LOrderProduct(Base):
    table: str = 'l_order_product'
    hk_order_product_pk: UUID
    h_order_pk: UUID
    h_product_pk: UUID
    

class LProductRestaurant(Base):
    table: str = 'l_product_restaurant'
    hk_product_restaurant_pk: UUID
    h_product_pk: UUID
    h_restaurant_pk: UUID
    
class LProductCategory(Base):
    table: str = 'l_product_category'
    hk_product_category_pk: UUID
    h_product_pk: UUID
    h_category_pk: UUID
