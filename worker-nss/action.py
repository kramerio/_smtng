import asyncio
import json
from telnet_client import TelnetClient
from utils import get_arp, fix_port
import logging
import traceback

logger = logging.getLogger(__name__)

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
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
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
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
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
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
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
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
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
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
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
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
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
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
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
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
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
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
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
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
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
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
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
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
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
        data["error"] = {"code": 100, "msg":f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
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
        data["error"] = {"code": 100,"msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)


#
#   ETHERNET
#
#todo думаю стоит ли обьеденить process_clear_switch_ports и process_clear_switch_port в одну
# делают то они одно и тоже...
async def process_clear_switch_ports(task_data, id):
    """
    Очищает acl и vlan'ы с указанных портов в task_data['ports'].

    :param task_data: payload задачи.
    :param id: ИД воркера, который выполняет задачу.

    :return: возвращаем ответ на задачу (int, str)
    """
    # Парсим payload на отдельные переменные для удобства
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    ports = _tmp['ports']   # [1, 2, ...]

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
        logger.error(f"[Consumer id:{id}]process_clear_switch_ports: Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)

    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")
        # каждый порт очищаем
        for port in ports:
            # Проверка на корректность порта
            try:
                port = fix_port(port)
            except Exception as e:
                logger.error(f"[Consumer id:{id}]process_clear_switch_ports: Failed to fix port; Error: {e}")
                continue

            try:
                response = await asyncio.to_thread(tc.port_clear, port)  # True / False
                logger.info(f"[Consumer id:{id}]process_clear_switch_ports: port_clear={port}: {response}")
            except Exception as e:
                logger.error(f"[Consumer id:{id}]process_clear_switch_ports: port={port}; Error - {e}")
                continue

        data["success"] = True
        data["response"] = True

        # Отключаемся
        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}]process_clear_switch_ports: Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)

async def process_clear_switch_port(task_data, id):
    """
    Очищает порт на свитче от acl и vlan'ов.

    :param task_data: payload задачи.
    :param id: ИД воркера, который выполняет задачу.

    :return: возвращаем ответ на задачу (int, str)
    """
    # Парсим payload на отдельные переменные для удобства
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']
    port = _tmp['port']


    data = {
        "success": False,
        "response": None,
        "error": None
    }

    # Проверка на корректность порта
    # Иногда в либре есть порты типа 122, 108 и т.п, но по сути это порты 1:22 и 1:08
    # А иногда и четырех значные числа, но это уже бред
    try:
        port = fix_port(port)
    except Exception as e:
        logger.error(f"[Consumer id:{id}]process_clear_switch_port: Failed to fix port; Error: {e}")
        data["error"] = {"code": 5001, "msg": f"Failed to fix port {port}; Error: {e}"}
        return 1, json.dumps(data)

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}]process_clear_switch_port: Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)

    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")

        response = await asyncio.to_thread(tc.port_clear, port)  # True / False
        logger.info(f"[Consumer id:{id}] response={response}")

        data["success"] = True
        data["response"] = response

        # Отключаемся
        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}]process_clear_switch_port: Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
        return 1, json.dumps(data)


async def process_port_test(task_data, id):
    """
    Тестирования порта на свитче.

    :param task_data: payload задачи
    :param id: ИД воркера, который выполняет задачу

    :return: Возвращаем ответ на задачу (int, str)
    """
    # Парсим payload на отдельные переменные
    _tmp = json.loads(task_data)
    _device_ip = _tmp['device_ip']
    _model = _tmp['device_name']
    _community = _tmp['community']

    ip, port, vlan, mode, aserver_ip = _tmp['bind'] # [ip, port, vlan, mode, aserver.ip]

    # подготавливаем список для результатов выполнения диагностики
    response = {
        "vlans": None,
        "speed": None,
        "macs": None,
        "diag": None,
        "state": None,
        "arp": None
    }
    # Подготавливаем payload для ответа
    data = {
        "success": False,
        "response": None,
        "error": None
    }

    # Проверка на корректность порта
    # Иногда в либре есть порты типа 122, 108 и т.п, но по сути это порты 1:22 и 1:08
    # А иногда и четырех значные числа, но это уже бред
    try:
        port = fix_port(port)
    except Exception as e:
        logger.error(f"[Consumer id:{id}]process_port_test: Failed to fix port; Error: {e}")
        data["error"] = {"code": 5001, "msg": f"Failed to fix port {port}; Error: {e}"}
        return 1, json.dumps(data)

    try:
        tc = await asyncio.to_thread(
            lambda: TelnetClient(_model, _device_ip, None, _community.split(':')[0],
                                 _community.split(':')[1])
        )
        await asyncio.to_thread(tc.connect)

    except Exception as e:
        logger.error(f"[Consumer id:{id}]process_port_test: Не удалось подключиться к свитчу {_model}({_device_ip}). Error: {e}")
        data["error"] = {"code": 100, "msg": f"An error occurred while connecting to the switch {_model}({_device_ip}). Error: {e}"}
        return 1, json.dumps(data)

    if await asyncio.to_thread(tc.auth):
        logger.info(f"[Consumer id:{id}] Подключился к свитчу {_model}({_device_ip})")
        response["is_available"] = True
        # собираем инфу
        response["vlans"] = await asyncio.to_thread(tc.get_vlans, port) # [vlan, vlan, ...]
        response["speed"] = await asyncio.to_thread(tc.get_speed, port)  # dict{tx:0,rx:0} or None
        response["macs"] = await asyncio.to_thread(tc.get_macs, port)  # [mac, mac, ...]
        response["state"] = await asyncio.to_thread(tc.get_state, port)  # (bool, bool)
        response["diag"] = await asyncio.to_thread(tc.cable_diag, port)  # dict{lst: match.group("lst"), clen: match.group("clen"), pair: pairs if pairs else None}
        response["arp"] = await asyncio.to_thread(get_arp, ip, vlan, mode, aserver_ip)  # str or False

        data["success"] = True
        data["response"] = response

        # Отключаемся
        await asyncio.to_thread(tc.disconnect)

        logger.info(f"[Consumer id:{id}] Работа завершена")
        return 0, json.dumps(data)

    else:
        logger.error(f"[Consumer id:{id}]process_port_test: Не удалось авторизоваться на свитче {_model}({_device_ip})")
        data["error"] = {"code": 101, "msg": f"Failed to login to switch {_model}({_device_ip})"}
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