import asyncio
import os
import random
import uuid
from datetime import datetime
from db_client import DBClient
from rabbit_client import QueueManager
from loger import Log
import weakref
import json
from loger import configure_logging, configure_dps_logger
import logging
import traceback


activity = [0.0, 0.0]
# список задач в работе
futures = weakref.WeakValueDictionary()
pool_tasks = []
# список заблокированных задач
banned = dict()
# самописный класс для работы с БД
db_client = DBClient('***', ***, '***', '***', '***')
dps_logger = configure_dps_logger()

async def watchdog():
    """
    Функция, которая проверяет не завис ли основной процесс.
    Если время последнего прохода цикла не изменилась за 1 минуту,
    то считаем основную функцию зависшей и убиваем себя.

    Вроде не актуально, не было зависаний больше
    :return: ¯\_(ツ)_/¯
    """
    global activity
    while True:
        await asyncio.sleep(60 * 1)
        # если текущее значение равно предыдущему
        if activity[1] == activity[0]:
            logger.error(f"[Master] Основная функция не отвечала более минуты! Убиваю себя =(")
            await asyncio.sleep(3)
            os._exit(1) #type: ignore
        else:
            activity[0] = activity[1]

def remove_track_task(task):
    """
    Удаляет из списка задачу на отслеживание и разбан, чтобы Task мог быть собран сборщиком мусора
    :param task: track_task
    :return: ¯\_(ツ)_/¯
    """
    try:
        pool_tasks.remove(task)
        if task.cancelled():
            logger.warning(f"[Master] Задача функции была отменена: {task.get_name()}")
        elif task.exception():
            logger.warning(f"[Master] Задача функции {task.get_name()} завершилась с исключением: {task.exception()}")
        else:
            logger.info(f"[Master] Задача функции {task.get_name()} успешно завершилась")
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"[Master] При удалении задачи функции {task.get_name()} произошла ошибка! {e} - {error_details}", exc_info=True)

async def timeout_task(key):
    """
    Функция, которая после 60 * 10 (мин) удаляет из списка futures задачу.
    Если задача висит более 10 мин, значит что-то пошло не так и что бы не ломать
    работу мы просто удаляем ее
    :param key: ИД задачи (task_id)
    :return: ¯\_(ツ)_/¯
    """
    await asyncio.sleep(60 * 10)
    try:
        # Пробуем установить ответ, чтобы функция track_futures завершила задачу
        # Этот ответ вызвет Except т.к в ответе после : не json строка
        # Но нам Except скорее на руку
        future: asyncio.Future = futures.get(key)
        future.set_result(b'1:task was destroyed by timeout!')
        logger.warning(f"Задача {key} преждевременно удалена по таймауту")
        # Если задача уже не отслеживается track_futures, но есть в futures, просто тупо удаляем ее
        # Но ждем 20 секунд, чтобы track_futures успела отработать
        await asyncio.sleep(20)
        futures.pop(key)
    except Exception as e:
        pass

async def unban(key):
    """
    Функция, которая через 60 * 5 сек (5 мин) удалить забаненный task_id,
    что бы его снова можно было взять в работу
    :param key: ИД задачи (task_id)

    :return: ¯\_(ツ)_/¯
    """
    # спим
    await asyncio.sleep(60 * 5)
    # Удаляем со списка заблокированных
    try:
        banned.pop(key)
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"[Master] unban error! {error_details}\n", exc_info=True)

async def unsuccessful_task(result_task: dict):
    """
    Функция, которая обрабатывает НЕ успешно завершенные задачи
    :param result_task: ответ от worker'a

    :return: ¯\_(ツ)_/¯
    """
    # Просто записываем лог задачи
    info = result_task['info']
    dps_logger.info(info)

async def completed_task(result_task: dict):
    """
    Функция, которая обрабатывает успешно завершенные задачи

    :param result_task: ответ от worker'a

    :return: ¯\_(ツ)_/¯
    """

    # Лог выполнения задачи
    info = result_task['info']
    # Ответ воркера на задачу
    result: list = result_task['result']


    if isinstance(result[0], list):
        # когда result список списков, то ответ работы со свитчем
        for item in result:
            match item[1]:
                case 'DELETED':
                    await db_client.delete_record(item[2])
                case 'MODIFY':
                    await db_client.modify_record(item[2])

    elif isinstance(result, list):
        # когда result это просто список знат гейпон
        match result[1]:
            case 'DELETED_GPON':
                await db_client.delete_record_gpon(result[2])
            case 'MODIFY_GPON':
                await db_client.modify_record_gpon(result[2])
    else:
        logger.warning(f"[Master] Получен неизвестный объект для обработки (type:{type(result)}, data:{result})")

    dps_logger.info(info)


async def track_futures(task_id, loop):
    """
    Функция для отслеживания запущенных задач

    :param task_id: уникальный ИД задачи
    :param loop:    куратина асинхронного цикла

    :return: ¯\_(ツ)_/¯
    """

    # Получаем задачу
    future = futures.get(task_id)
    if future is None:
        logger.info(f"[Master] Задача {task_id} не найдена в futures!")
        return
    logger.info(f"[Master] Отслеживаю задачу {task_id}...")
    # Ожидаем завершение задачи
    try:
        result = await asyncio.wait_for(future, timeout=600000)

    except asyncio.TimeoutError:
        logger.error(f"[Master] Timeout waiting for response for task {task_id}")
        banned[task_id] = 'lock'
        futures.pop(task_id)
        # Удаляем с блок-листа после 5 минут
        # Делаем ссылку на задачу, чтобы сборщик мусора не полмал нам ее
        _task = loop.create_task(unban(task_id), name=f"unban-{task_id}")
        _task.add_done_callback(remove_track_task)
        pool_tasks.append(_task)

        return


    logger.info(f"[Master] Задача {task_id} вернула данные: {result.decode('utf-8').encode().decode('unicode_escape')}")

    # Парсим результат выполнения задачи
    code, json_part = result.split(b':', 1)
    code = int(code)

    if json_part == b"task was destroyed by timeout!": # костыль (?)
        futures.pop(task_id)
        return

    result_task: dict = json.loads(json_part.decode('utf-8'))

    # Если код ответа 0 и ответ задачи True(бля...не тру тут сложно кароч) фикшу), то считаем задачу успешно завершенной
    if code == 0 and result_task['response']:
        await completed_task(result_task)
        logger.info(f"[Master] Задача {task_id} выполнена и удалена из отслеживания")
        return

    # Если что-то не так, то считаем задачу завершенной, но не успешно
    await unsuccessful_task(json_part)
    logger.warning(f"[Master] Задача {task_id} выполнена, но не успешно! result: {result.decode('utf-8').encode().decode('unicode_escape')}")
    banned[task_id] = 'lock'
    futures.pop(task_id)
    # Удаляем с блок-листа после 5 минут
    # Делаем ссылку на задачу, чтобы сборщик мусора не полмал нам ее
    _task = loop.create_task(unban(task_id), name=f"unban-{task_id}")
    _task.add_done_callback(remove_track_task)
    pool_tasks.append(_task)

    return


async def create_task(loop):
    """
    Основная функция, которая вызывается при старте. Отслеживает кол-во задач в работе
    и при их отсутствии попробует создать новые, если такие есть
    :param loop: куратина асинхронного цикла
    :return: ничего не возвращаем
    """
    _task = loop.create_task(watchdog(), name=f"watchdog")
    _task.add_done_callback(remove_track_task)
    pool_tasks.append(_task)

    # Работаем
    while True:

        # logger.info(f"[Master] Кручусь! Ожидаю работу!")
        # Если задач меньше 10, то входим в условие
        if len(futures) < 10:
            try:
                # Получаем список задач для синхронизации ethernet абонентов
                switchs_to_sync = await db_client.get_switch_list_to_sync(list(banned.keys()) + list(futures.keys()))
                # Получаем список задач для синхронизации gpon абонентов
                work_gpon = await db_client.get_work_gpon(list(banned.keys()) + list(futures.keys()))
                # Мы живы!
                activity[1] = datetime.now().timestamp()

                # task = {
                # "device_ip":row["device_ip"],
                # "community":row["community"],
                # "device_name": row["device_name"],
                # "binds": row["bind_info"]
                # }
                for task in switchs_to_sync:
                    activity[1] = datetime.now().timestamp()
                    # Самописный класс для работы с очередью
                    queue_manager = QueueManager(futures, db_client)

                    task_id = task["device_ip"] + f"-{uuid.uuid4()}"
                    task["task_id"] = task_id
                    logger.info(f"[Master] Создаю задачу: {task}")

                    # Отправляем задачу в очередь
                    future = loop.create_task(
                        queue_manager.send_to_queue("sync_switch_tasks", task),
                        name=f"send-task-{task_id}"
                    )
                    # Делаем ссылку на задачу, чтобы сборщик мусора не поламал нам ее
                    futures[task_id] = future

                    # Запускаем отслеживание только что запущенной задачи
                    _task = loop.create_task(track_futures(task_id, loop), name=f"track-{task_id}")
                    _task.add_done_callback(remove_track_task)

                    pool_tasks.append(_task)
                    # По таймауту удаляем задачу
                    _task = loop.create_task(timeout_task(task_id), name=f"timeout-track-{task_id}")
                    _task.add_done_callback(remove_track_task)

                    pool_tasks.append(_task)

                # todo переделать как на switchs_to_sync
                # Обрабатываем каждую задачу из списка
                for work in work_gpon:
                    activity[1] = datetime.now().timestamp()

                    # Самописный класс для работы с очередью
                    queue_manager = QueueManager(futures, db_client)

                    logger.info(f"[Master] Создаю задачу: {work}")
                    task_id = f"{work['ip']}-{uuid.uuid4()}"
                    work['task_id'] = task_id
                    # Отправляем задачу в очередь
                    future = loop.create_task(
                        queue_manager.send_to_queue("sync_nat_tasks", work),
                        name=f"send-task-{task_id}"
                    )
                    futures[task_id] = future
                    # Запускаем отслеживание только что запущенной задачи

                    # Делаем ссылку на задачу, чтобы сборщик мусора не поламал нам ее
                    _task = loop.create_task(track_futures(task_id, loop), name=f"track-{task_id}")
                    _task.add_done_callback(remove_track_task)
                    pool_tasks.append(_task)
                    # По таймауту удаляем задачу
                    _task = loop.create_task(timeout_task(task_id), name=f"timeout-track-{task_id}")
                    _task.add_done_callback(remove_track_task)
                    pool_tasks.append(_task)
            except:
                error_details = traceback.format_exc()
                logger.error(f"[Master] При создании задачи произошла ошибка! {error_details}", exc_info=True)

        if len(futures)+len(banned) > 0:
            logger.info(f"[Master] work tasks: {list(futures.keys())}")
            logger.info(f"[Master] ban tasks: {list(banned.keys())}")


        # Задержка, чтобы процессору было не так больно
        await asyncio.sleep(5)

async def main():
    await db_client.create_pool()
    loop = asyncio.get_running_loop()
    _ = await loop.create_task(create_task(loop), name=f"main-task")

    await asyncio.Future()

if __name__ == '__main__':
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("App started")
    # Взлетаем!
    asyncio.run(main())
