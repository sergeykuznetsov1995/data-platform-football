"""Run the planner's literal environment without native Jinja conversion."""

from airflow.operators.bash import BashOperator


class TransfermarktBashOperator(BashOperator):
    """The TM planner already resolves every environment value to text.

    Native rendering can turn numeric strings into integers and JSON into
    dictionaries before Popen. Environment values are signed/data payloads,
    not templates: keep their original bytes, including hashes and braces.
    The command and any other BashOperator template fields render normally.
    """

    template_fields = tuple(field for field in BashOperator.template_fields if field != 'env')
