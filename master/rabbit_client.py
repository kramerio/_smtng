import asyncio
from aio_pika import Message, connect
import json
import logging

logger = logging.getLogger(__name__)

class QueueManager:
    """
    Управление очередью RabbitMQ
    """
    # инициализация
    def __init__(self, futures, db_client):
        self.consumer_tag = None
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.futures = futures
        self.db_client = db_client
        self._connection_lock = asyncio.Lock()

    # async def __aexit__(self, exc_type, exc, tb):
    #     await self.close()

    # подключение к серверу rabbitmq
    async def connect(self):
        # Защищаем подключение от конкурентного вызова
        async with self._connection_lock:
            # Если соединение уже установлено и активно, возвращаем объект
            if self.connection is not None and not self.connection.is_closed:
                return self

            # Перед созданием нового соединения закрываем старое, если оно есть
            # await self.close()

            logger.info("[Master] Подключаюсь к БД для получения данных авторизации...")
            rpc_host = '***'
            rpc_user = '***' #await self.db_client.get_data_variable('rpc_user')
            rpc_password = '***' #await self.db_client.get_data_variable('rpc_password')

            logger.info("[Master] Подключаюсь к очереди RabbitMQ...")
            try:
                # Устанавливаем соединение с RabbitMQ
                self.connection = await connect(f"amqp://{rpc_user}:{rpc_password}@{rpc_host}/")
                self.channel = await self.connection.channel()
            except Exception as e:
                logger.error(f"[Master] Ошибка подключения к RabbitMQ, Error {e}", exc_info=True)
                raise

            try:
                # Создаем временную очередь для получения ответов
                self.callback_queue = await self.channel.declare_queue(exclusive=True,
                                                                       arguments={
                                                                            "x-message-ttl": 30000,     # сообщения удаляются через 30 секунд
                                                                            "x-expires": 600000,         # очередь удаляется, если не используется 600 секунд (10 min)
                                                                        })
                logger.info(f"[Master] Создаю временную очередь для ответа воркера {self.callback_queue.name}")
                # Подписываемся на сообщения из очереди
                self.consumer_tag = await self.callback_queue.consume(self.on_response, no_ack=True)
            except Exception as e:
                logger.error(f"[Master] Ошибка при создании очереди или подписке, Error: {e}", exc_info=True)
                raise

            return self


    async def on_response(self, message) -> None:
        # Обрабатываем входящие сообщения
        if message.correlation_id is None or message.correlation_id not in self.futures:
            logger.warning(f"[Master] Bad message {message!r}")
            return
        # logger.info(f"[Master] Пришел ответ (raw): {message!r}")
        future: asyncio.Future = self.futures.get(message.correlation_id)
        # if future is not None and not future.done():
        #     future.set_result(message.body)
        future.set_result(message.body)

        await self.close()


    async def send_to_queue(self, queue_name, payload: dict):
        """
        Отправка сообщения в очередь.
        Если соединение не активно – происходит переподключение.
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        _id = payload['task_id']

        # Грузим в массив задачу
        self.futures[_id] = future

        try:
            # Если канал не установлен или закрыт, переподключаемся
            if self.connection is None or self.connection.is_closed or self.channel is None or self.channel.is_closed:
            # if self.channel is None or self.channel.is_closed:
                await self.connect()

            # Публикуем сообщение в указанную очередь
            await self.channel.default_exchange.publish(
                Message(
                    #
                    body=(json.dumps(payload)).encode(),
                    content_type="text/plain",
                    correlation_id=_id,
                    reply_to=self.callback_queue.name,
                ),
                routing_key=queue_name,
            )
            # ждем завершение задачи... Но зачем...?
            # А не зачем, тут была какая-то идея но сейчас это уже не актульано
            # return await future
            #return future  # Возвращаем future, чтобы можно было дождаться ответа
        except Exception as e:
            logger.error(f"[Master] Ошибка при отправке сообщения в новую очередь, Error: {e}", exc_info=True)
            return -1


    async def close(self) -> None:
        """Закрываем канал и соединение, если они открыты."""

        try:
            await self.callback_queue.cancel(self.consumer_tag)
            await self.callback_queue.delete()
            self.callback_queue = None
        except Exception as e:
            logger.error(f"[Master] Ошибка при закрытии callback очереди RabbitMQ, Error: {e}", exc_info=True)

        try:
            if self.channel is not None and not self.channel.is_closed:
                await self.channel.close()
                self.channel = None
        except Exception as e:
            logger.error(f"[Master] Ошибка при закрытии канала RabbitMQ, Error: {e}", exc_info=True)

        try:
            if self.connection is not None and not self.connection.is_closed:
                await self.connection.close()
                self.connection = None
        except Exception as e:
            logger.error(f"[Master] Ошибка при закрытии подключения к RabbitMQ, Error: {e}", exc_info=True)
