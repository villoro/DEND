from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#B3E5FC"

    @apply_defaults
    def __init__(self, table=None, query_select=None, redshift_conn_id="redshift", *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        # Query params
        self.table = table
        self.query_select = query_select

        # Hooks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):

        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f"Loading '{self.table}' dimension table")
        redshift.run(f"INSERT INTO {self.table} ({self.query_select});")
