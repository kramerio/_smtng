import asyncio
import json
from telnet_client import TelnetClient
import logging

logger = logging.getLogger(__name__)

def add_log(condition: bool, success_msg: str, failure_msg: str) -> str:
    return success_msg if condition else failure_msg
# todo хочу отказатся от варианта с tasks(наверное, ибо в action он там скорее лишний, а тут выглядит не так страшно). Нужно выделить денек-два
async def help_delete(tc, ip, port, vlan, mode):

    """
    Очищает порт, если бинд был удален

    :param tc: объект TelnetClient
    :param ip: на данный момент не используется
    :param port: порт, с которого нужно все почистить
    :param vlan: на данный момент не используется
    :param mode: на данный момент не используется
    :return: возвращаем логи действий
    """
    log_parts = []

    # Список действий для выполнения и логирования
    tasks = [
        {
            "action": lambda: asyncio.to_thread(tc.ip_filter_del, port),
            "results": 2,
            "messages": [
                (0, f"|Очищен от acl правила(permit) порт {port} (NSS)\n",
                 f"|Не удалось очистить acl правило(permit) с порта {port} (NSS)\n"),
                (1, f"|Очищен от acl правила(deny) порт {port} (NSS)\n",
                 f"|Не удалось очистить acl правило(deny) с порта {port} (NSS)\n"),
            ]
        },
        {
            "action": lambda: asyncio.to_thread(tc.vlans_del, port),
            "results": 3,
            "success": f"|Очищен от влана(ов) порт {port} (NSS)\n",
            "failure": f"|Не удалось очистить от влана(ов) либо нет ни одного влана порт {port} (NSS)\n",
            "combine": lambda results: any(results),  # Условие успешности
        }
    ]

    # Выполнение задач и логирование
    for task in tasks:
        results = await task["action"]()
        if task["results"] == 1:
            # Для одиночного результата
            log_parts.append(add_log(results, task["success"], task["failure"]))
        elif task.get("combine"):
            # Для комбинированных результатов
            log_parts.append(add_log(task["combine"](results), task["success"], task["failure"]))
        elif "messages" in task:
            # Для индивидуального логирования по каждому результату
            for idx, success_msg, failure_msg in task["messages"]:
                log_parts.append(add_log(results[idx], success_msg, failure_msg))

    return "".join(log_parts)

async def help_modify(tc, ip, port, vlan, mode):
    """
    Синхронизирует информации на порту относительно БД

    :param tc: объект TelnetClient
    :param ip: ip абонента, нужен для создания acl
    :param port: порт, с которым работаем
    :param vlan: влан абонента (инет, нат)
    :param mode: абстрактоое значение, указывающая должен ли быть доступ к интернету (0 - нет, 1 - да)
    :return: возвращаем логи действий
    """
    log_parts = []
    vlan = vlan if mode else 'fake'

    # Список действий для выполнения и логирования
    tasks = [
        {
            "action": lambda: asyncio.to_thread(tc.vlans_del, port),
            "results": 3,  # Количество возвращаемых значений (result_inet, result_nat, result_fake)
            "success": f"|Очищен от влана(ов) порт {port} (NSS)\n",
            "failure": f"|Не удалось очистить от влана(ов) либо нет ни одного влана порт {port} (NSS)\n",
            "combine": lambda results: any(results),  # Условие успешности
        },
        {
            "action": lambda: asyncio.to_thread(tc.ip_filter_del, port),
            "results": 2, # Количество возвращаемых значений (profile_2, profile_3)
            "messages": [
                (0, f"|Очищен от acl правила(permit) порт {port} (NSS)\n",
                 f"|Не удалось очистить acl правило(permit) с порта {port} (NSS)\n"),
                (1, f"|Очищен от acl правила(deny) порт {port} (NSS)\n",
                 f"|Не удалось очистить acl правило(deny) с порта {port} (NSS)\n"),
            ]
        },
        {
            "action": lambda: asyncio.to_thread(tc.ip_filter_add, port, ip),
            "results": 2, # (profile_2, profile_3)
            "messages": [
                (0, f"|Добавлено acl правило(permit) на порт {port} (NSS)\n",
                 f"|Не удалось добавить acl правило(permit) на порт {port} (NSS)\n"),
                (1, f"|Добавлено acl правило(deny) на порт {port} (NSS)\n",
                 f"|Не удалось добавить acl правило(deny) на порт {port} (NSS)\n"),
            ]
        },
        {
            "action": lambda: asyncio.to_thread(tc.vlan_add, port, vlan),
            "results": 1, # (result)
            "success": f"|Добавлен влан {vlan} на порт {port} (NSS)\n",
            "failure": f"|Не удалось добавить влан {vlan} на порт {port} (NSS)\n",
        }
    ]

    # Выполнение задач и логирование
    for task in tasks:
        results = await task["action"]()
        if task["results"] == 1:
            # Для одиночного результата
            log_parts.append(add_log(results, task["success"], task["failure"]))
        elif task.get("combine"):
            # Для комбинированных результатов
            log_parts.append(add_log(task["combine"](results), task["success"], task["failure"]))
        elif "messages" in task:
            # Для индивидуального логирования по каждому результату
            for idx, success_msg, failure_msg in task["messages"]:
                log_parts.append(add_log(results[idx], success_msg, failure_msg))

    return "".join(log_parts)



# todo хочу изменить data = { } ответа
#  и возможно пересмотреть алгоритм распределения delete и modify
async def process_sync(task_data, id):
    """
    Основная функция для работы с задачами полученными от master

    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param id: ИД воркера, который выполняет задачу
    :return: (int, str) возвращаем код результата и json строку ответа
    """
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']

    binds = _tmp['binds']

    result = []
    log = ''
    data = {
        "device_ip": _device_ip,
        "response": False,
        "info": '',
        "result": result
    }
    await asyncio.sleep(3)
    logger.info(f"[Consumer id:{id}] Авторизация на свитче {_model}({_device_ip})...")
    # Подключение к свитчу
    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)
    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_device_ip}", exc_info=True)
        log = f'[Не удалось подключится к свитчу {_device_ip} (NSS). Ошибка: {e}\n'
        data['info'] = log
        return 1, json.dumps(data)

    # Авторизация на свитче
    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_device_ip}")
        log = f'/Подключился к свитчу {_device_ip} (NSS)\n'
        # Обрабатываем каждого клиента на свитче
        for bind in binds:
            # Переменные для работы с каждым абонентом
            _ip, _port, _vlan, _mode, _type, _id, _parent = bind
            log += f'|Начало работы с услугой {_parent} (NSS)\n'
            try:
                logger.info(f"[Consumer id:{id}] Тип задачи: {_type}")
                if len(str(_port)) == 3:
                    _port = f"{str(_port)[:-2]}:{str(_port)[-2:]}"
                elif len(str(_port)) > 3:
                    raise f"Номер порта не может быть таким {_port}"
                match _type:
                    # Определяем что нужно сделать
                    case 'DELETED':
                        log += await help_delete(tc, _ip, _port, _vlan, _mode)
                        result.append([_ip, _type,_id])
                    case 'MODIFY':
                        log += await help_modify(tc, _ip, _port, _vlan, _mode)
                        result.append([_ip, _type,_id])
            except Exception as e:
                log += f'|Работа со свитчем пошла не по плану (NSS)! Ошибка: {e}\n'
        try:
            if await asyncio.to_thread(tc.save):
                log += f'|Конфигурация сохранена (NSS)\n'
            else:
                log += f'|Не удалось сохранить конфигурацию (NSS)\n'
            await asyncio.to_thread(tc.disconnect)
        except Exception as e:
            log += f'\Сохранение пошло не по плану (NSS)! Ошибка: {e}\n'
            data['info'] = log
            return 1, json.dumps(data)
        log += f'\Отключился от свитча (NSS)\n'
        data['response'] = True
        data['result'] = result
        data['info'] = log
        logger.info(f"[Consumer id:{id}] Задача завершена")
        return 0, json.dumps(data)
    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_device_ip}", exc_info=True)
        log += f'[Не удалось авторизоваться на свитче {_device_ip} (NSS)!\n'
        data['info'] = log
        return 1, json.dumps(data)

