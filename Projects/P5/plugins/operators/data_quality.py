from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#C5E1A5"

    query = """
        SELECT
            count(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_nulls,
            count(*) AS n_total
        FROM {table}
    """

    @apply_defaults
    def __init__(self, dict_checks=None, redshift_conn_id="redshift", *args, **kwargs):
        """
            Operator that checks the data quality. It needs a dict_checks with the checks:
                * key:      table name
                * value:    list with columns that must be not null
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # Checks
        self.dict_checks = dict_checks

        # Hooks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):

        redshift = PostgresHook(self.redshift_conn_id)

        errors = {}
        for table, columns in self.dict_checks.items():

            self.log.info(f"Checking that columns {columns} are not null for table '{table}'")

            errors[table] = {}
            for col in columns:
                records = redshift.get_records(self.query.format(table=table, col=col))

                if len(records) < 1 or len(records[0]) < 1:
                    errors[table][col] = "No results"
                    log.error(f"Data quality check failed. {table} returned no results")
                    continue  # Do the next column

                num_records = records[0][0]
                if num_records > 0:
                    errors[table][col] = f"There are nulls {num_records} nulls"
                    log.error(f"Data quality check failed. {table} contains {num_records} nulls")

        # The idea is to first check all errors and then raise the exception with all info
        if errors:
            raise ValueError(f"There are errors: {errors}")
