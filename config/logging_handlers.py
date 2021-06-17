from logging import Handler, LogRecord

import telebot


class TelegramBotHandler(Handler):
    def __init__(self, token: str, chat_id: str):
        super().__init__()
        self.token = token
        self.chat_id = chat_id
        self.context = None

    def emit(self, record: LogRecord):
        bot = telebot.TeleBot(self.token)
        bot.send_message(
            self.chat_id,
            f'DAG: {self.context.dag_id}\n'
            f'Execution date: {self.context.execution_date}\n'
            f'Task: {self.context.task_id}\n'
            f'Error: {self.format(record)}'
        )

    def set_context(self, context):
        self.context = context
