from airflow.hooks.oracle_hook import OracleHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class OracleToOracleTransfer(BaseOperator):
    template_fields = ('source_sql', 'source_sql_params')
    template_ext = '.sql'
    ui_color = '#e08c8c'
    @apply_defaults
    def __init__(
            self,
            oracle_destination_conn_id,
            destination_table,
            oracle_source_conn_id,
            source_sql,
            source_sql_params=None,
            rows_chunk=5000,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        if source_sql_params is None:
            source_sql_params = {}
        self.oracle_destination_conn_id = oracle_destination_conn_id
        self.destination_table = destination_table
        self.oracle_source_conn_id = oracle_source_conn_id
        self.source_sql = source_sql
        self.source_sql_params = source_sql_params
        self.rows_chunk = rows_chunk
    

    def _execute(self, src_hook, dest_hook, context):
        with src_hook.get_conn() as src_conn:
            cursor=src_conn.cursor()
            self.log.info("Querying data from source: %s", self.oracle_source_conn_id)
            cursor.execute(self.source_sql, self.source_sql_params)
            target_fields = list(map(lambda field: field[0], cursor.description))
            rows_total = 0
            rows = cursor.fetchmany(self.rows_chunk)
            while len(rows) > 0:
                rows_total = rows_total + len(rows)
                dest_hook.bulk_insert_rows(self.destination_table, rows,
                        target_fields=target_fields,
                        commit_every=self.rows_chunk)
                rows = cursor.fetchmany(self.rows_chunk)
                self.log.info("Total inserted: %s rows", rows_total)
            self.log.info("Finished data transfer.")
            cursor.close()

    def execute(self, context):
        src_hook = OracleHook(oracle_conn_id=self.oracle_source_conn_id)
        dest_hook = OracleHook(oracle_conn_id=self.oracle_destination_conn_id)
        self._execute(src_hook, dest_hook, context)
            
