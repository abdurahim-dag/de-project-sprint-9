from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert(
        self,
        table_name: str,
        params: dict,
        on_conflict: list
    ) -> None:
        column_names = list(params.keys())
        columns = ','.join(column_names)
        values = str([str(v) for v in params.values()])[1:-1]
        conflict_cols = ','.join(on_conflict)
        conflict_sets = ''

        # Генерируем вложение, для DO UPDATE SET
        # column = EXCLUDED.column, ...
        for i in range(len(column_names)):
            if columns[i] not in on_conflict:
                conflict_sets += f"{column_names[i]} = excluded.{column_names[i]},"

        sql = f"""
            insert into cdm.{table_name}({columns}) values ({values})
            on conflict ({conflict_cols}) do update
            SET {conflict_sets[:-1]};
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
