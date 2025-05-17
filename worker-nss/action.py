import asyncio
import json
from telnet_client import TelnetClient
from utils import get_arp
import logging
import traceback

logger = logging.getLogger(__name__)

def fix_port(raw_port) -> str:
    _port = raw_port
    if len(str(raw_port)) == 3:
        _port = f"{str(raw_port)[:-2]}:{str(raw_port)[-2:]}"
    elif len(str(raw_port)) > 3:
        raise f"Номер порта не может быть таким {raw_port}"
    return str(_port)

def add_log(condition: bool, success_msg: str, failure_msg: str) -> str:
    return success_msg if condition else failure_msg

#
#   GPON
#
async def process_onu_test(task_data, id):
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _serial = _tmp['serial_num']

    # phy_port, vport, status, mac, level, errors = None, None, None, None, None

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": None, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)

    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        onu_information: dict = await asyncio.to_thread(tc.show_onu_information_sn, _serial, False)
        # "IntfName":     [2, 29],
        # "VendorID":     "HWTC",
        # "ModelID":      "PU-X910",
        # "SN":           "HWTC:1D80B640",
        # "LOID":         "N/A",
        # "Status":       "active",
        # "ConfigStatus": "success",
        # "ActiveTime":   "2025-01-17 15:32:14",

        if not onu_information:
            logger.error(f"[Consumer id:{id}] Не удалось получить информацию про ONU [{_serial}].")
            data["error"] = {"code": 1001, "msg": f"Failed to get ONU information [{_serial}]"}
            return 1, json.dumps(data)

        intf = onu_information.get("IntfName")  # [phy_port, vport]
        phy_port, vport = intf

        onu_errors_packets = await asyncio.to_thread(tc.show_error, phy_port, vport)
        # "rx_sec": <int>,
        # "tx_sec": <int>,
        # "rx_total": <int>,
        # "tx_total": <int>,
        # "burst": <int>,
        # "bip8": <int>,
        # "lcdg": <int>,
        # "rdi": <int>,
        mac = await asyncio.to_thread(tc.show_mac, phy_port, vport)
        # str
        lvl = await asyncio.to_thread(tc.show_lvl, phy_port, vport)
        # (rx, tx)

        await asyncio.to_thread(tc.disconnect)

        response = {**onu_information, **onu_errors_packets, "mac": mac, "lvl": lvl}

        data["success"] = True
        data["response"] = response

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 1000, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)


async def process_show_onu_information_interface(task_data, id):
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _phy_port = _tmp['phy_port']

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": None, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)

    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        response = await asyncio.to_thread(tc.show_onu_information_interface, _phy_port)

        data["success"] = True
        data["response"] = response

        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 1000, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)

async def process_unconfigure_onu(task_data, id):
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _serial = _tmp['serial_num']
    _phy_port = _tmp['phy_port']

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": None, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)


    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        onu_information = await asyncio.to_thread(tc.show_onu_information_sn, _serial, False)
        # "IntfName":     [2, 29],
        # "VendorID":     "HWTC",
        # "ModelID":      "PU-X910",
        # "SN":           "HWTC:1D80B640",
        # "LOID":         "N/A",
        # "Status":       "active",
        # "ConfigStatus": "success",
        # "ActiveTime":   "2025-01-17 15:32:14",
        if onu_information:
            # Проверяем ону в сети
            if onu_information["Status"] == "active":
                logger.warning(f"[Consumer id:{id}] ONU [{_serial}] в сети. Нельзя удалить связку и удалить конфиг.")
                data["error"] = {"code": 1004, "msg": f"ONU [{_serial}] is online. Cannot remove configure"}
                return 1, json.dumps(data)

        res = await asyncio.to_thread(tc.unregister_onu, _serial, _phy_port)
        if not res:
            logger.error(f"[Consumer id:{id}] Failed to remove ONU config [{_serial}].")
            data["error"] = {"code": 2001, "msg": f"Failed to remove ONU config [{_serial}]"}
            return 1, json.dumps(data)

        data["success"] = True
        data["response"] = True

        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 1000, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)

async def process_reconfigure_onu(task_data, id):
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _serial = _tmp['serial_num']
    _phy_port = _tmp['phy_port']
    _vlan = _tmp['vlan']

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": None, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)


    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        res = await asyncio.to_thread(tc.unregister_onu, _serial, _phy_port)
        if not res:
            logger.error(f"[Consumer id:{id}] Failed to remove ONU config [{_serial}].")
            data["error"] = {"code": 2001, "msg": f"Failed to remove ONU config [{_serial}]"}
            return 1, json.dumps(data)

        # Получаем информацию про онушку
        onu_information = await asyncio.to_thread(tc.show_onu_information_sn, _serial, False)  # dict
        # "IntfName":     [2, 29],
        # "VendorID":     "HWTC",
        # "ModelID":      "PU-X910",
        # "SN":           "HWTC:1D80B640",
        # "LOID":         "N/A",
        # "Status":       "active",
        # "ConfigStatus": "success",
        # "ActiveTime":   "2025-01-17 15:32:14",
        #
        if not onu_information:
            logger.error(f"[Consumer id:{id}] Не удалось получить информацию про ONU [{_serial}].")
            data["error"] = {"code": 1001, "msg": f"Failed to get ONU information [{_serial}]"}
            return 1, json.dumps(data)

        intf = onu_information.get("IntfName")  # [phy_port, vport]
        phy_port, vport = intf

        # Проверяем, что ону в сети
        if onu_information["Status"] != "active":
            logger.warning(f"[Consumer id:{id}] ONU [{_serial}] не в сети. Нельзя настроить конфиг.")
            data["error"] = {"code": 1002, "msg": f"ONU [{_serial}] is offline. Cannot configure"}
            return 1, json.dumps(data)

        # Накидываем конфиг
        is_register = await asyncio.to_thread(tc.register_onu, _vlan, phy_port, vport)
        if not is_register:
            logger.error(f"[Consumer id:{id}] ONU [{_serial}] не удалось сконфигурировать.")
            data["error"] = {"code": 1003, "msg": f"ONU [{_serial}] could not be configured."}
            return 1, json.dumps(data)

        # Еще раз получаем статус ОНУ, для того чтобы вернуть это в ответ
        onu_information = await asyncio.to_thread(tc.show_onu_information_sn, _serial, False)

        data["success"] = True
        data["response"] = onu_information

        # Отключаемся
        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 1000, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)

async def process_configure_onu(task_data, id):
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _vlan = _tmp['vlan']
    _serialpon = _tmp['serial_num']


    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": None, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)

    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        # Получаем информацию про онушку
        onu_information = await asyncio.to_thread(tc.show_onu_information_sn, _serialpon, False)  # dict
        # "IntfName":     [2, 29],
        # "VendorID":     "HWTC",
        # "ModelID":      "PU-X910",
        # "SN":           "HWTC:1D80B640",
        # "LOID":         "N/A",
        # "Status":       "active",
        # "ConfigStatus": "success",
        # "ActiveTime":   "2025-01-17 15:32:14",
        #
        if not onu_information:
            logger.error(f"[Consumer id:{id}] Не удалось получить информацию про ONU [{_serialpon}].")
            data["error"] = {"code": 1001, "msg": f"Failed to get ONU information [{_serialpon}]"}
            return 1, json.dumps(data)

        intf = onu_information.get("IntfName")  # [phy_port, vport]
        phy_port, vport = intf

        # Проверяем, что ону в сети
        if onu_information["Status"] != "active":
            logger.warning(f"[Consumer id:{id}] ONU [{_serialpon}] не в сети. Нельзя настроить конфиг.")
            data["error"] = {"code": 1002, "msg": f"[{_serialpon}] is offline. Cannot configure"}
            return 1, json.dumps(data)

        # Накидываем конфиг
        is_register = await asyncio.to_thread(tc.register_onu, _vlan, phy_port, vport)
        if not is_register:
            logger.warning(f"[Consumer id:{id}] ONU [{_serialpon}] не удалось сконфигурировать.")
            data["error"] = {"code": 1003, "msg": f"ONU [{_serialpon}] could not be configured."}
            return 1, json.dumps(data)

        # Еще раз получаем статус ОНУ, для того чтобы вернуть это в ответ
        onu_information = await asyncio.to_thread(tc.show_onu_information_sn, _serialpon, False)

        data["success"] = True
        data["response"] = onu_information

        # Отключаемся
        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 1000, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)

async def process_show_mac_onu(task_data, id):
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _phy_port = _tmp['phy_port']
    _vport = _tmp['vport']

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": None, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)


    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        res = await asyncio.to_thread(tc.show_mac, _phy_port, _vport)
        data["success"] = True
        data['response'] = {"mac": res}

        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 1000, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)

async def process_show_onu_information_sn(task_data, id):
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _serialpon = _tmp['serial_num']

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code":None, "msg":f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)

    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        res = await asyncio.to_thread(tc.show_onu_information_sn, _serialpon, False) # dict
        #"IntfName":     [2, 29],
        #"VendorID":     "HWTC",
        #"ModelID":      "PU-X910",
        #"SN":           "HWTC:1D80B640",
        #"LOID":         "N/A",
        #"Status":       "active",
        #"ConfigStatus": "success",
        #"ActiveTime":   "2025-01-17 15:32:14",
        if res:
            intf = res.get("IntfName") # [phy_port, vport]
            phy_port, vport = intf
            data["success"] = True
            data["response"] = res

        # Отключаемся
        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}] Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": None,"msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)


#
#   ETHERNET
#

#todo привести инет функции +- к виду как у gpon
# хочу отказатся от варианта с tasks. Нужно выделить денек-два и сделать по меньше try except'ов.
# Но для этого нужно более четко понимать dlink.py и extreme.py, а к ним руки доходить не хотят
async def process_clear_switch_ports(task_data, id):
    """
    Функция, которая передует синхронизации. Очищает вланы и acl с указанных портов в task_data['bind']
    :param task_data: данные для задачи
    :param id: id worker'a
    :return: код результата, ответ
    """
    # Парсим payload на отдельные переменные для удобства
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _ports = _tmp['bind']

    data = {"response": False, "log": ''}
    # Пробуем подключится к свитчу
    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(
                _model,
                _device_ip,
                None,
                _community.split(":")[0],
                _community.split(":")[1],
            )
        )
        await asyncio.to_thread(tc.connect)
    except Exception as e:
        return 1, json.dumps(data)

    # Авторизация на свитче
    if await asyncio.to_thread(tc.auth):
        is_success = True
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        for port in _ports:
            # Проверка на корректность порта
            try:
                port = fix_port(port)
            except Exception as e:
                logger.error(f"[Consumer id:{id}]process_clear_switch_ports: Error - {e}")
                continue

            try:
                await asyncio.to_thread(tc.port_clear, port)
            except Exception as e:
                logger.error(f"[Consumer id:{id}]process_clear_switch_ports: port={port}; Error - {e}")
                continue

        try:
            await asyncio.to_thread(tc.disconnect)
        except Exception as e:
            is_success = False
    else:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_device_ip}")
        is_success = False

    logger.info(f"[Consumer id:{id}] Задача завершена")
    # Если все ок, возвращаем код 0 и нагрузку, иначе код 1 и нагрузку
    if is_success:
        data["response"] = True
        return 0, json.dumps(data)
    else:
        return 1, json.dumps(data)


async def process_clear_switch_port(task_data, id):
    """
    Функция для очистки указаного порта в payload. Вызывается только при сохранении связки,
    чтобы удалить старые настройки с порта
    :param task_data: payload задачи
    :param id: ИД номер воркера, который выполняет задачу
    :return: возвращаем ответ на задачу (int, str)
    """
    log_parts = []
    # Парсим payload на отдельные переменные для удобства
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    _port = _tmp['bind']
    # подготавливаем список для результатов выполнения диагностики
    data = {"response": False, "log": None}

    # Список функций, которые будут выполнены
    tasks = [
        {
            "action": lambda tc: asyncio.to_thread(tc.port_clear, _port),
            "results": 2,
            "success": f"|Порт {_port} очищен успешно!\n",
            "failure": f"|Не удалось очистить порт {_port}!\n",
        },
    ]
    # Проверка на корректность порта
    try:
        _port = fix_port(_port)
    except Exception as e:
        log_parts.append(f"[Работа со свитчем пошла не по плану! Ошибка: {e}\n")
        log = "".join(log_parts)
        data['log'] = log
        return 1, json.dumps(data)

    # Пробуем подключится к свитчу
    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(
                _model,
                _device_ip,
                None,
                _community.split(":")[0],
                _community.split(":")[1],
            )
        )
        await asyncio.to_thread(tc.connect)
    except Exception as e:
        log_parts.append(f"[Не удалось подключится к свитчу {_device_ip}. Ошибка: {e}\n")
        log = "".join(log_parts)
        data['log'] = log
        return 1, json.dumps(data)

    is_success = False
    # Авторизация на свитче
    if await asyncio.to_thread(tc.auth):
        is_success = True
        log_parts.append(f'/Подключился к свитчу {_device_ip}\n')
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")
        # Выполнение задач и запись их результатов
        for task in tasks:
            try:
                results = await task["action"](tc)
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
                if not results:
                    log_parts.append(f"\Работа со свитчем пошла не по плану!\n")
                    is_success = False
                    break  # Прекращаем выполнение на первой ошибке
            except Exception as e:
                log_parts.append(f"\Работа со свитчем пошла не по плану! Ошибка: {e}\n")
                is_success = False
                break
        try:
            await asyncio.to_thread(tc.disconnect)
            log_parts.append(f'\Отключился от свитча\n')
        except Exception as e:
            pass
            # log_parts.append(f'\Сохранение пошло не по плану! Ошибка: {e}\n')
            # is_success = False
    else:
        log_parts.append(f"[Не удалось подключиться к свитчу {_device_ip}\n")
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_device_ip}")
        is_success = False

    log = "".join(log_parts)
    data['log'] = log
    logger.info(f"[Consumer id:{id}] Задача завершена")
    # Если все ок, возвращаем код 0 и нагрузку, иначе код 1 и нагрузку
    if is_success:
        data["response"] = True
        return 0, json.dumps(data)
    else:
        return 1, json.dumps(data)

async def process_port_test(task_data, id):
    """
    Функция тестирования порта.
    :param task_data: payload задачи
    :param id: ИД номер воркера, который выполняет задачу
    :return: Возвращаем ответ на задачу (int, str)
    """
    log_parts = []

    # Парсим payload на отдельные переменные для удобства
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    bind = _tmp['bind']

    # подготавливаем список для результатов выполнения диагностики
    results = {
        "vlans": None,
        "speed": None,
        "macs": None,
        "diag": None,
        "state": None,
        "arp": None
    }
    # Подготавливаем payload для ответа
    data = {
        "response": False,
        "log": None
    }
    # Список функций, которые будут выполнены
    tasks = [
        {"action": lambda tc: asyncio.to_thread(tc.get_vlans, bind[1]), "result": "vlans"},
        {"action": lambda tc: asyncio.to_thread(tc.get_speed, bind[1]), "result": "speed"},
        {"action": lambda tc: asyncio.to_thread(tc.get_macs, bind[1]), "result": "macs"},
        {"action": lambda tc: asyncio.to_thread(tc.get_state, bind[1]), "result": "state"},
        {"action": lambda tc: asyncio.to_thread(tc.cable_diag, bind[1]), "result": "diag"},
        {"action": lambda tc: asyncio.to_thread(get_arp, bind[0], bind[2], bind[3], bind[4]), "result": "arp"},
    ]
    # Проверка на корректность порта
    # Иногда в либре есть порты типа 122, 108 и т.п, но по сути это порты 1:22 и 1:08
    # А иногда и четырех значные числа, но это уже бред
    try:
        bind[1] = fix_port( bind[1])
    except Exception as e:
        log_parts.append(f"[Работа со свитчем пошла не по плану! Ошибка: {e}\n")
        log = "".join(log_parts)
        data['log'] = log
        return 1, json.dumps(data)

    # Пробуем подключится к свитчу
    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)
    except Exception as e:
        log_parts.append(f"[Не удалось подключиться к свитчу {_device_ip}. Ошибка: {e}\n")
        log = "".join(log_parts)
        data['log'] = log
        return 1, json.dumps(data)

    # Авторизация на свитче
    if await asyncio.to_thread(tc.auth):
        log_parts.append(f'/Подключился к свитчу {_device_ip}\n')
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")
        log_parts.append(f'|Сбор данных с порта {bind[1]}...\n')
        # Выполнение задач и запись их результатов
        is_available = True
        for task in tasks:
            try:
                result = await task["action"](tc)
                results[task["result"]] = result
            except Exception as e:
                log_parts.append(f"|Ошибка при выполнении задачи {task['result']}: {e}\n")
                break
        try:
            await asyncio.to_thread(tc.disconnect)
            log_parts.append(f"\Отключился от свитча\n")
        except Exception as e:
            log_parts.append(f"\Ошибка при отключении от свитча: {e}\n")

    else:
        logger.error(f"[Consumer id:{id}] Не удалось подключиться к свитчу {_device_ip}")
        log_parts.append(f"[Не удалось подключиться к свитчу {_device_ip}\n")
        is_available = False

    # Формируем окончательный вариант ответа
    data = {
        "is_available": is_available,
        "state": results["state"],
        "speed": results["speed"],
        "vlans": results["vlans"],
        "diag": results["diag"],
        "macs": results["macs"],
        "arp": results["arp"],
        "response": True
    }
    log = "".join(log_parts)
    data['log'] = log
    logger.info(f"[Consumer id:{id}] Задача завершена")
    # Если все ок, возвращаем код 0 и нагрузку, иначе код 1 и нагрузку
    if is_available:
        return 0, json.dumps(data)
    else:
        return 1, json.dumps(data)

async def process_diagnostic(task_data, id):
    """
    Функция, которая определяет тип полученной задачи.
    Адресует payload в нужную функцию для дальнейшей обработки

    :param task_data: payload задачи
    :param id: ИД номер воркера, который выполняет задачу. Нужен лишь для логов

    :return: (int, str) код выполнения задачи и ответ
    """
    task = json.loads(task_data)['task']
    match task:
        # GPON
        case 'show_onu_information_interface':
            return await process_show_onu_information_interface(task_data, id)

        case 'show_onu_information':
            return await process_show_onu_information_sn(task_data, id)

        case 'show_mac_onu':
            return await process_show_mac_onu(task_data, id)

        case 'configure_onu':
            return await process_configure_onu(task_data, id)

        case 'reconfigure_onu':
            return await process_reconfigure_onu(task_data, id)

        case 'unconfigure_onu':
            return await process_unconfigure_onu(task_data, id)

        case 'onu_test':
            return await process_onu_test(task_data, id)

        # ETHERNET
        case 'port_test':
            return await process_port_test(task_data, id)

        case 'clear_switch_port':
            return await process_clear_switch_port(task_data, id)

        case 'clear_switch_ports':
            return await process_clear_switch_ports(task_data, id)

        case _:
            logger.error(f"[Consumer id:{id}] задача {task} не реализована")
            return 1, 1