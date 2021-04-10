from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG['handlers'].update(
    {
        'telegram_handler': {
            'class': 'logging_handlers.TelegramBotHandler',
            'chat_id': '<CHAT_ID>',
            'token': '<BOT_TOKEN>',
            'level': 'ERROR',
        }
    }
)
LOGGING_CONFIG['loggers']['airflow.task']['handlers'].append('telegram_handler')
