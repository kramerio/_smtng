import asyncio
import json
from telnet_client import TelnetClient
from utils import fix_port
import logging

logger = logging.getLogger(__name__)

async def help_delete(tc, port) -> str:

    """
    Очищает порт, если бинд был удален

    :param tc: объект TelnetClient
    :param port: порт, с которого нужно все почистить

    :return: возвращаем логи действий
    """
    log = ""

    # удаляем вланы с порта
    vlans = await asyncio.to_thread(tc.vlans_del, port)  # (bool, bool, bool) inet, nat, fake
    vlan_res = any(vlans)

    # удаляем acl с порта
    acl_permit, acl_deny = await asyncio.to_thread(tc.ip_filter_del, port)  # (bool, bool)       permit, deny

    if acl_permit:
        log += f"|Очищен от acl правила(permit) порт {port}\n"
    else:
        log += f"|Не удалось очистить acl правило(permit) с порта {port}\n"

    if acl_deny:
        log += f"|Очищен от acl правила(deny) порт {port}\n"
    else:
        log += f"|Не удалось очистить acl правило(deny) с порта {port}\n"

    if vlan_res:
        log += f"|Очищен от влана(ов) порт {port}\n"
    else:
        log += f"|Не удалось очистить от влана(ов) либо нет ни одного влана порт {port}\n"

    return log

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

    # сначала удалим на порту все, что бы по новой настроить
    log = await help_delete(tc, port)

    # добавляем acl на порт
    acl_permit, acl_deny = await asyncio.to_thread(tc.ip_filter_add, port, ip)  # (bool, bool)  permit, deny

    # определим какой влан накинуть
    vlan = vlan if mode else 'fake'
    # добавляем влан на порт
    vlan_res = await asyncio.to_thread(tc.vlan_add, port, vlan)  # bool

    if acl_permit:
        log += f"|Добавлено acl правило(permit) на порт {port}\n"
    else:
        log += f"|Не удалось добавить acl правило(permit) на порт {port}\n"

    if acl_deny:
        log += f"|Добавлено acl правило(deny) на порт {port}\n"
    else:
        log += f"|Не удалось добавить acl правило(deny) на порт {port}\n"

    if vlan_res:
        log += f"|Добавлен влан {vlan} на порт {port}\n"
    else:
        log += f"|Не удалось добавить влан {vlan} на порт {port}\n"

    return log

async def process_sync(task_data, worker_id):
    """
    Основная функция для работы с задачами полученными от master

    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param worker_id: ИД воркера, который выполняет задачу
    :return: (int, str) возвращаем код результата и json строку ответа
    """
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']

    binds = _tmp['binds']


    data = {
        "log": "",
        "success": False,
        "response": None,
        "error": None,
    }

    # Подключаемся к свитчу
    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        log = f"[Не удалось подключится к свитчу {_model}({_device_ip}). Ошибка: {e}\n"
        data["log"] = log

        logger.error(f"[Consumer id:{worker_id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)


    # Авторизация на свитче
    if await asyncio.to_thread(tc.auth):
        log = f"/Подключился к свитчу {_model}({_device_ip})\n"
        response = []

        logger.info(f"[Consumer id:{worker_id}] Подключился к свитчу {_model}({_device_ip})")

        for bind in binds:
            # Переменные для работы с каждым абонентом
            ip, port, vlan, mode, _type, bind_id = bind # [bind_ip, bind_port, bind_vlan, type_task, bind_id]
            log += f"|Начало работы с портом {port}\n"

            try:
                logger.info(f"[Consumer id:{worker_id}] Тип задачи: {_type}")
                port = fix_port(port)

                match _type:
                    case "DELETED": # если тип задачи DELETED, то очищаем порт, а мастер удаляет запись в кэше
                        log += await help_delete(tc, port)
                        # response.append([_type, bind_id])

                    case "MODIFY": # если тип задачи MODIFY, то очищаем порт и настраиваем, а мастер обновляет запись в кэше
                        log += await help_modify(tc, ip, port, vlan, mode)
                        # response.append([_type, bind_id])

            except Exception as e:
                log += f"|Работа с портом {port} пошла не по плану! Ошибка: {e}\n"

            # добавляем в список выполненную задачу
            response.append([_type, bind_id])

        try:
            if await asyncio.to_thread(tc.save):
                log += f"\Конфигурация сохранена\n"
            else:
                log += f"\Не удалось сохранить конфигурацию(?)\n"
            await asyncio.to_thread(tc.disconnect)

        except Exception as e:
            log += f"\Сохранение пошло не по плану! Ошибка: {e}\n"
            data["log"] = log

            data["error"] = {"code": 100, "msg": f"An error occurred while saving the switch configuration {_model}({_device_ip}). Error: {e}"}
            return 1, json.dumps(data)

        data["success"] = True
        data["response"] = response # [[type, bind_id], [], []... ]
        data["log"] = log

        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{worker_id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{worker_id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)
