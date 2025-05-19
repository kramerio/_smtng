import asyncio
import json
import logging

logger = logging.getLogger(__name__)

async def block_ip(ip: str, mac: str, worker_id) -> bool:
    """
    Добавляет IP в список ipset'a

    :param ip: IP, который нужно добавить в список.
    :param mac: РАБОТА С МАК АДРЕСАМИ ПОКА НЕ РЕАЛИЗОВАННА.
    :param worker_id: ИД воркера, который выполняет задачу.

    :return: Возвращаем True или False
    """
    logger.info(f"[Consumer id:{worker_id}] Добавляю в blocklist IP {ip}")
    try:
        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset add blocklist %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] add blocklist stdout={stdout}; stderr={stderr}")

        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset list | grep -w %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] ipset list stdout={stdout}; stderr={stderr}")

        # Если вывод есть, возвращаем True, иначе выводим False
        if stdout.decode().strip():
            logger.info(f"[Consumer id:{worker_id}] Добавление IP {ip} в blocklist прошло успешно")
            return True

        logger.warning(f"[Consumer id:{worker_id}] Добавление IP {ip} в blocklist НЕ прошло успешно")
        return False

    except Exception as e:
        logger.error(f"[Consumer id:{worker_id}] Error", exc_info=True)
        return False

async def unblock_ip(ip: str, mac: str, worker_id)-> bool:
    """
    Удаляет IP из списка ipset'a

    :param ip: IP, который нужно удалить из списка.
    :param mac: РАБОТА С МАК АДРЕСАМИ ПОКА НЕ РЕАЛИЗОВАННА.
    :param worker_id: ИД воркера, который выполняет задачу.

    :return: Возвращаем True или False
    """
    logger.info(f"[Consumer id:{worker_id}] Удаляю из blocklist IP {ip}")
    try:
        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset del blocklist %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] del blocklist stdout={stdout}; stderr={stderr}")

        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset list | grep -w %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] ipset list stdout={stdout}; stderr={stderr}")

        # Если вывод пустой, возвращаем True, иначе выводим False
        if not stdout.decode().strip():
            logger.info(f"[Consumer id:{worker_id}] Удаление IP {ip} из blocklist прошло успешно")
            return True

        logger.warning(f"[Consumer id:{worker_id}] Удаление IP {ip} из blocklist НЕ прошло успешно")
        return False

    except Exception as e:
        logger.error(f"[Consumer id:{worker_id}] Error", exc_info=True)
        return False

async def modify(ip: str, mac: str, mode: int, worker_id) -> bool:
    """
    Функция прослойка, нужна для определения того нужно заблокировать
    либо разблокировать IP. Может, будут еще какие-то варианты

    :param ip: IP, с которым проходит работа.
    :param mac: РАБОТА С МАК АДРЕСАМИ ПОКА НЕ РЕАЛИЗОВАННА.
    :param mode: По моду определяем нужно заблокировать либо разблокировать.
    :param worker_id: ИД воркера, который выполняет задачу.

    :return: Возвращаем то, что вернут вызванные функции
    """
    if mode:
        return await unblock_ip(ip, mac, worker_id)
    else:
        return await block_ip(ip, mac, worker_id)

async def process_sync(task_data, worker_id):
    """
    Основная функция для работы с задачами полученными от master

    :param task_data: Полезная нагрузка, данные которые нужно обработать.
    :param worker_id: ИД воркера, который выполняет задачу.

    :return: Возвращаем код результата и полезную нагрузку для ответа.
    """
    pl = json.loads(task_data)
    ip = pl['ip']
    _type = pl['change_type']

    data = {
        "log": "",
        "success": False,
        "response": None,
        "error": None,
    }
    log = f"/Выполняю задачу \'{_type}\' с IP {ip} на NAT-сервере\n"
    result = False
    response = []
    match _type:
        case 'DELETED_GPON':
            log += f"|Пытаюсь заблокировать IP, в связи с отсутствием связки\n"
            result = await block_ip(ip, pl['mac'], worker_id)

        case 'MODIFY_GPON':
            log += f"|Пытаюсь {"разблокировать" if pl['mode'] else "заблокировать"} IP\n"
            result = await modify(ip, pl['mac'], int(pl['mode']), worker_id)

    if result:
        log += f"\Выполнения задачи успешно\n"
        data["log"] = log

        data["success"] = True
        response.append([_type, pl['id']])
        data["response"] = response # [[type, bind_id]]
        return 0, json.dumps(data)

    log += f"\Выполнения задачи НЕ успешно\n"
    data["log"] = log

    logger.error(f"[Consumer id:{worker_id}] Не удалось выполнить задачу {_type} с IP {ip}. result={result}")
    data["error"] = {"code": 9001, "msg": f"Failed to complete task {_type} with IP {ip}."}
    return 1, json.dumps(data)