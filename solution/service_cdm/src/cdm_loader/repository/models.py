from uuid import UUID

from pydantic import BaseModel, Field


class Base(BaseModel):
    user_id: UUID
    order_cnt: int


class UserCategoryCounters(Base):
    table: str = 'user_category_counters'
    on_conflict: list = ['user_id', 'category_id']
    id: UUID = Field(alias='category_id')
    name: str = Field(alias='category_name')

    class Config:
        allow_population_by_field_name = True


class UserProductCounters(Base):
    table: str = 'user_product_counters'
    on_conflict: list = ['user_id', 'product_id']
    id: UUID = Field(alias='product_id')
    name: str = Field(alias='product_name')

    class Config:
        allow_population_by_field_name = True
