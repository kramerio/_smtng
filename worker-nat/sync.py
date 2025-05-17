import asyncio
import json
import logging

logger = logging.getLogger(__name__)

def add_log(condition: bool, success_msg: str, failure_msg: str) -> str:
    return success_msg if condition else failure_msg

async def block_ip(ip: str, mac: str, id) -> bool:
    """
    Функция для добавления определенного IP в список ipset'a

    :param ip: IP, который нужно добавить в список
    :param mac: РАБОТА С МАК АДРЕСАМИ ПОКА НЕ РЕАЛИЗОВАННА
    :param id: ИД воркера, который выполняет задачу

    :return: Возвращаем True или False
    """
    try:
        logger.info(f"[Consumer id:{id}] Добавляю в blocklist IP {ip}")
        p1 = await asyncio.create_subprocess_shell(
            '/sbin/ipset add blocklist %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await p1.communicate()
        logger.info(f"[Consumer id:{id}] Проверяю наличие IP {ip} в blocklist")
        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset list | grep -w %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()

        # Если вывод есть, возвращаем True, иначе выводим False
        if stdout.decode().strip():
            logger.info(f"[Consumer id:{id}] Добавление IP {ip} в blocklist прошло успешно")
            return True
        logger.warning(f"[Consumer id:{id}] Добавление IP {ip} в blocklist НЕ прошло успешно")
        return False
    except Exception as e:
        logger.error(f"[Consumer id:{id}] Error", exc_info=True)
        return False

async def unblock_ip(ip: str, mac: str, id)-> bool:
    """
    Функция для удаления определенного IP из списка ipset'a

    :param ip: IP, который нужно удалить из списка
    :param mac: РАБОТА С МАК АДРЕСАМИ ПОКА НЕ РЕАЛИЗОВАННА
    :param id: ИД воркера, который выполняет задачу

    :return: Возвращем True или False
    """
    try:
        logger.info(f"[Consumer id:{id}] Удаляю из blocklist IP {ip}")
        p1 = await asyncio.create_subprocess_shell(
            '/sbin/ipset del blocklist %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await p1.communicate()
        logger.info(f"[Consumer id:{id}] Проверяю наличие IP {ip} в blocklist")
        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset list | grep -w %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()

        # Если вывод пустой, возвращаем True, иначе выводим False
        if not stdout.decode().strip():
            logger.info(f"[Consumer id:{id}] Удаление IP {ip} из blocklist прошло успешно")
            return True
        logger.warning(f"[Consumer id:{id}] Удаление IP {ip} из blocklist НЕ прошло успешно")
        return False
    except Exception as e:
        logger.error(f"[Consumer id:{id}] Error", exc_info=True)
        return False

async def modify(ip: str, mac: str, mode: int, id) -> bool:
    """
    Функция прослойка, нужна для определения того нужно заблокировать
    либо разблокировать IP. Может будут еще какие-то варианты

    :param ip: IP, с которым проходит работа
    :param mac: РАБОТА С МАК АДРЕСАМИ ПОКА НЕ РЕАЛИЗОВАННА
    :param mode: По моду определяем нужно заблокировать либо разблокировать
    :param id: ИД воркера, который выполняет задачу
    :return: возвращаем то, что вернут вызванные функции
    """
    if mode:
        return await unblock_ip(ip, mac, id)
    else:
        return await block_ip(ip, mac, id)

async def process_sync(task_data, id):
    """
    Основная функция для работы с задачами полученными от master
    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param id: ИД номер воркера, который выполняет задачу
    :return: возвращаем код результата и полезную нагрузку для ответа
    """
    _tmp = json.loads(task_data)
    _ip = _tmp['ip']
    _mac = _tmp['mac']
    _type = _tmp['change_type']
    _mode = _tmp['mode']
    _id = _tmp['id']
    result = False
    log = ''
    data = {
        "ip": _ip,
        "response": False,
        "info": '',
        "result": []
    }
    await asyncio.sleep(3)
    log = f"/Выполняю задачу \'{_type}\' с IP {_ip} на NAT-сервере (NSS)\n"
    # Исходя от типа задачи распределяем логику
    match _type:
        case 'DELETED_GPON':
            log += f"|Пытаюсь заблокировать IP, в связи с отсутствием связки (NSS)\n"
            result = await block_ip(_ip, _mac, id)
        case 'MODIFY_GPON':
            log += f"|Пытаюсь {"разблокировать" if _mode else "заблокировать"} IP (NSS)\n"
            result = await modify(_ip, _mac, int(_mode), id)

    if result:
        log += f"\Выполнения задачи успешно (NSS)\n"
        data['info'] = log
        data['response'] = True
        data['result'] = [_ip, _type, _id, _mac, int(_mode)]
        return 0, json.dumps(data)
    log += f"\Выполнения задачи НЕ успешно (NSS)\n"
    data['info'] = log
    return 1, json.dumps(data)