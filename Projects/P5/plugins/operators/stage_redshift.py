from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    query = """
        COPY {table} FROM '{path_s3}'
        ACCESS_KEY_ID '{aws_id}' SECRET_ACCESS_KEY '{aws_password}'
        IGNOREHEADER {ignore_headers}
        DELIMITER '{delimiter}'
    """

    @apply_defaults
    def __init__(
        self,
        table=None,
        path_s3=None,
        delimiter=",",
        ignore_headers=1,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        *args,
        **kwargs,
    ):

        # Check input params
        for name, param in [("table", table), ("path_s3", path_s3)]:
            if param is None:
                msg = f"{name.title()} param must be not None"
                log.error(msg)
                raise ValueError(msg)

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Query params
        self.table = table
        self.path_s3 = path_s3
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

        # Hooks
        self.redshift = PostgresHook(redshift_conn_id)
        self.aws = AwsHook(aws_credentials_id).get_credentials()

    def execute(self, context):

        # Delete data if present
        self.log.info("Clearing data from destination Redshift table")
        self.redshift.run("DELETE FROM {}".format(self.table))

        # Insert data from s3
        self.log.info(f"Copying data from {self.path_s3} to Redshift")
        self.redshift.run(
            self.query.format(
                table=self.table,
                path_s3=self.path_s3,
                aws_id=self.aws.access_key,
                aws_password=self.aws.secret_key,
                ignore_headers=self.ignore_headers,
                delimiter=self.delimiter,
            )
        )
