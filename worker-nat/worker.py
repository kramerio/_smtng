import functools
import aio_pika
from action import *
from sync import *
from loger import *


AMQP_LINK = "amqp://****/"

async def process_task(task_data, worker_id, cmd):
    """
    Функция прослойка, для распределения логики.

    :param task_data: Полезная нагрузка, данные которые нужно обработать.
    :param worker_id: ИД воркера, который выполняет задачу.
    :param cmd: ИД команды, который нужно выполнить.

    :return: (КОД ОТВЕТА, полезная нагрузка ответа)
    """
    match cmd:
        # Задачи, которые дает либра
        case 'diagnostic_nat_tasks':
            return await process_diagnostic(task_data, worker_id)
        # Задачи, которые дает мастер
        case 'sync_nat_tasks':
            return await process_sync(task_data, worker_id)
        # Если ИД команды не найден
        case _:
            logger.error(f"[Consumer id:{worker_id}] Некорректный запрос: отсутствует реализация текущей команды: {cmd}")
            return 1, 'Queue not found'

async def process_message(message: aio_pika.IncomingMessage, worker_id=None, cmd=None):
    """
    Функция обработки полученного сообщения из очереди.

    :param message: Само сообщение класа aio_pika.IncomingMessage.
    :param worker_id: ИД воркера, который выполняет задачу.
    :param cmd: ИД команды, которую нужно выполнить.

    :return: ¯\_(ツ)_/¯
    """
    # используем with message.process для автоматической отправки ack после завершения обработки
    async with message.process():
        request_data = message.body.decode()
        logger.info(f"[Consumer id:{worker_id}] Получена задача {cmd}")
        logger.info(f"[Consumer id:{worker_id}] Тело задачи: {request_data}")

        # Обрабатываем входящую задачу, получаем response_code, response_data
        response_code, response_data = await process_task(request_data, worker_id, cmd)

        # Если что-то не то пришло:
        if not message.reply_to and not message.correlation_id:
            logger.error(f"[Consumer id:{worker_id}] Некорректный запрос: отсутствует reply_to или correlation_id")
            return 1
        logger.info(f"[Consumer id:{worker_id}] correlation_id, reply_to: {message.properties.correlation_id}, {message.properties.reply_to}")
        logger.info(f"[Consumer id:{worker_id}] response_code, response_data: {response_code}, {response_data}")
        # Исходя из полученной задачи формирует разного вида ответ
        match cmd:
            case 'sync_nat_tasks':
                #Формируем ответ как RESULT_CODE:RESULT_DATA
                _body = f"{str(response_code)}:{str(response_data)}"
                response_message = aio_pika.Message(
                    body=_body.encode(),
                    content_type="text/plain",
                    correlation_id=message.properties.correlation_id
                )
            case 'diagnostic_nat_tasks':
                # Формируем ответ как RESULT_DATA
                response_message = aio_pika.Message(
                    body=str(response_data).encode(),
                    content_type="text/plain",
                    correlation_id=message.properties.correlation_id
                )
            case _:
                logger.error(f"[Consumer id:{worker_id}] Некорректный запрос: отсутствует реализация текущей команды: {cmd}")
                return 1
        try:
            # Шлем ответ, результат выполнения задачи
            await message.channel.basic_publish(
                body=response_message.body,
                exchange='',
                routing_key=message.properties.reply_to,
                properties=response_message.properties
            )
        except Exception as e:
            logger.error(f"[Consumer id:{worker_id}] Error process", exc_info=True)
            return 1

async def start_consumer(queue_name, amqp_url, worker_id):
    """
    Функция запуска слушателя.

    :param queue_name: Очередь, которую нужно слушать.
    :param amqp_url: url подключения к rabbitmq серверу.
    :param worker_id: ИД воркера, который выполняет задачу.

    :return: ¯\_(ツ)_/¯
    """

    # Настройки подключения к очереди
    connection = await aio_pika.connect_robust(amqp_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue(queue_name, durable=True)

    # Запускаем слушатель с калбек функцией process_message
    await queue.consume(functools.partial(process_message, worker_id=worker_id, cmd=queue_name))
    logger.info(f"[Consumer id:{worker_id}] Запущен слушатель")
    # Чтобы слушатель не умер
    await asyncio.Future()

async def main(amqp_url):
    """
    Главная функция, которая запускает в работу слушателей.

    :param amqp_url: url подключения к rabbitmq серверу.

    :return: По сути ничего возвращать не нужно
    """
    try:
        # Создаем n-ое кол-во слушателей для диагностических задач (которые получаем от либры)
        diagnostic_tasks = [asyncio.create_task(start_consumer('diagnostic_nat_tasks', amqp_url, f'{_}-diag')) for _ in range(10)]
        # Создаем n-ое кол-во слушателей для синхронизации (задачи которые получаем от мастера)
        sync_tasks = [asyncio.create_task(start_consumer('sync_nat_tasks', amqp_url, f'{_}-sync')) for _ in range(10)]

        # Стартауем это все говно
        await asyncio.gather(*diagnostic_tasks)
        await asyncio.gather(*sync_tasks)
    except Exception as e:
        logger.critical("Ошибка при подключении или работе с очередью", exc_info=True)
        await asyncio.sleep(15)  # В случае ошибок, прекращаем выполнение

if __name__ == '__main__':
    # Конфиг логера
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("App started")
    # Старт
    asyncio.run(main(AMQP_LINK))

