from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

# Change the default logging by changing the scheduler log
# handler to NullHandler, which doesn't log anything
LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG['handlers']['processor'] = {'class': 'logging.NullHandler'}
