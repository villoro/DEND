from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    query_insert

    @apply_defaults
    def __init__(self, table=None, query_select=None, redshift_conn_id="redshift", *args, **kwargs):

        # Check input params
        for name, param in [("table", table), ("query_select", query_select)]:
            if param is None:
                msg = f"{name.title()} param must be not None"
                log.error(msg)
                raise ValueError(msg)

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        # Query params
        self.table = table
        self.query_select = query_select

        # Hooks
        self.redshift = PostgresHook(redshift_conn_id)

    def execute(self, context):
        self.log.info(f"Loading '{self.table}' fact table")
        self.redshift.run(f"INSERT INTO {self.table} (self.query_select);")
