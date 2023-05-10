from datetime import datetime

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert(
        self,
        table_name: str,
        params: dict,
    ) -> None:
        """Функция шаблонной вставки в слой DDS."""
        columns = ','.join(params.keys())
        values = str([str(v) for v in params.values()])[1:-1]

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"insert into dds.{table_name}({columns}) values ({values}) on conflict do nothing"
                )
