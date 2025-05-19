import asyncio
import re
import json
import logging

logger = logging.getLogger(__name__)

# -A PREROUTING -d 109.95.54.15/32 -j DNAT --to-destination 10.41.51.4
# -A POSTROUTING -s 10.41.51.4/32 -o ens2.125 -j SNAT --to-source 109.95.54.15

PUBLIC_IP_ADD = {
    "PREROUTING":"/usr/sbin/iptables -t nat -I PREROUTING 3 -d {public_ip}/32 -j DNAT --to-destination {nat_ip}",
    "POSTROUTING":"/usr/sbin/iptables -t nat -I POSTROUTING 3 -s {nat_ip}/32 -o ens2.125 -j SNAT --to-source {public_ip}"
}
PUBLIC_IP_DEL = {
    "PREROUTING":"/usr/sbin/iptables -t nat -D PREROUTING -d {public_ip}/32 -j DNAT --to-destination {nat_ip}",
    "POSTROUTING":"/usr/sbin/iptables -t nat -D POSTROUTING -s {nat_ip}/32 -o ens2.125 -j SNAT --to-source {public_ip}"
}

async def unconfig_public_ip(task_data, worker_id):
    """
    Через iptables удаляет правило NAT, которое давало абоненту уникальный внешний IP

    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param worker_id: ИД воркера, который выполняет задачу

    :return: (int, str) код обработки и ответ json строка
    """
    _tmp = json.loads(task_data)
    ip = _tmp['ip']
    public_ip = _tmp['public_ip']

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    try:
        # проверим есть ли правило вообще
        process = await asyncio.create_subprocess_shell(
            f"/usr/sbin/iptables -vnL -t nat | grep -w {ip}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] Правило NAT table stdout={stdout}; stderr={stderr}")

        if not stdout.strip():
            data["success"] = True
            data["response"] = True
            logger.info(f"[Consumer id:{worker_id}] Правила NAT для {ip} не было")
            return 0, json.dumps(data)

        logger.info(f"[Consumer id:{worker_id}] Удаляю правило в NAT iptables для IP {ip}")
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_DEL["PREROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] Правило PREROUTING stdout={stdout}; stderr={stderr}")

        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_DEL["POSTROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] Правило POSTROUTING stdout={stdout}; stderr={stderr}")

        process = await asyncio.create_subprocess_shell(
            f"/usr/sbin/iptables -vnL -t nat | grep -w {ip}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] Правило NAT table stdout={stdout}; stderr={stderr}")

        data["success"] = True
        # Если вывода нет(правила добавлены), возвращаем True, иначе выводим False
        if not stdout.strip():
            data["response"] = True
            logger.info(f"[Consumer id:{worker_id}] Успешно удален NAT {ip} -> {public_ip}")
            return 0, json.dumps(data)

        else:
            data["response"] = False
            logger.info(f"[Consumer id:{worker_id}] НЕ успешно удален NAT {ip} -> {public_ip}")
            return 0, json.dumps(data)

    except Exception as e:
        logger.error(f"[Consumer id:{worker_id}]:config_public_ip: Error - {e}", exc_info=True)
        data["error"] = {"code": None, "msg": f"Error: {e}"}
        return 1, json.dumps(data)


async def config_public_ip(task_data, worker_id):
    """
    Через iptables настраивает правило NAT, для выхода в интернет с уникальным внешним IP.
    :param task_data: полезная нагрузка, данные которые нужно обработать.
    :param worker_id: ИД воркера, который выполняет задачу.
    :return: (int, str) код обработки и ответ json строка
    """
    # Перед добавлением нового правила, пробуем удалить старые, которые настроены на этот IP
    await unconfig_public_ip(task_data, worker_id)

    _tmp = json.loads(task_data)
    ip = _tmp['ip']
    public_ip = _tmp['public_ip']

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    logger.info(f"[Consumer id:{worker_id}] Добавляю правила в NAT iptables {ip} -> {public_ip}")
    try:
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_ADD["PREROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] Правило PREROUTING stdout={stdout}; stderr={stderr}")

        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_ADD["POSTROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] Правило POSTROUTING stdout={stdout}; stderr={stderr}")
        process = await asyncio.create_subprocess_shell(
            f"/usr/sbin/iptables -vnL -t nat | grep -w {ip}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{worker_id}] NAT table stdout={stdout}; stderr={stderr}")
        data['success'] = True

        # Если вывод есть(правила добавлены), возвращаем True, иначе выводим False
        if stdout.strip():
            data["response"] = True
            logger.info(f"[Consumer id:{worker_id}] Успешно настроен NAT {ip} -> {public_ip}")
            return 0, json.dumps(data)
        else:
            data["response"] = False
            logger.info(f"[Consumer id:{worker_id}] НЕ успешно настроен NAT {ip} -> {public_ip}; stdout={stdout}; stderr={stderr}")
            return 0, json.dumps(data)

    except Exception as e:
        logger.error(f"[Consumer id:{worker_id}]:config_public_ip: Error - {e}", exc_info=True)
        data["error"] = {"code": None, "msg": f"Error: {e}"}
        return 1, json.dumps(data)



async def get_arp(task_data, worker_id):
    """
    Получает arp запись по определенному IP и возвращает.

    :param task_data: полезная нагрузка, данные которые нужно обработать.
    :param worker_id: ИД воркера, который выполняет задачу.

    :return: (int, str) код обработки и ответ json строка
    """
    _tmp = json.loads(task_data)
    ip = _tmp['ip']

    ARP_LINE_RE = re.compile(
        r'''
        ^\S+                    # имя хоста (?)
        \s+\((?P<ip>[^)]+)\)    # IP
        \s+at\s+(?P<mac>\S+)    # MAC адрес или <incomplete>
        \s+\[.*?]\s+on\s+       # пропускаем [ether] и on
        (?P<interface>\S+)$     # имя интерфейса
        ''',
        re.VERBOSE
    )

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    logger.info(f"[Consumer id:{worker_id}] Проверяю наличие ARP записи с IP {ip}")
    try:
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            '/usr/sbin/arp -a -n | grep -w \(%s\)' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
    except Exception as e:
        logger.error(f"[Consumer id:{worker_id}] Не удалось выполнить команду '/usr/sbin/arp -a -n'. Error: {e}")
        data["error"] = {"code": 3001, "msg": f"The command could not be executed '/usr/sbin/arp -a -n'. Error: {e}"}
        return 1, json.dumps(data)

    logger.info(f"[Consumer id:{worker_id}] stdout={stdout}; stderr={stderr}")
    data['success'] = True

    # вывод команды
    result = stdout.decode().strip()
    # ищем по шаблону
    m = ARP_LINE_RE.match(result)

    if not m:
        logger.info(f"[Consumer id:{worker_id}] ARP запись с IP {ip} НЕ найдена")
        data["response"] = {"ip": ip, "mac": None, "interface": None}
        return 0, json.dumps(data)
    logger.info(f"[Consumer id:{worker_id}] ARP запись с IP {ip} найдена")
    data["response"] = m.groupdict()
    # {'ip': '10.41.42.7',
    # 'mac': 'b0:19:21:0d:62:8b',
    # 'interface': 'ens2.642'}
    return 0, json.dumps(data)


async def is_blocked(task_data, worker_id):
    """
    Проверяет нахождения определенного IP в ipset на NAT-сервере(тутс)

    :param task_data:   полезная нагрузка, данные которые нужно обработать
    :param worker_id:  ИД воркера, который выполняет задачу

    :return: (int, str) код обработки и ответ json строка
    """
    _tmp = json.loads(task_data)
    ip = _tmp['ip']
    mac = _tmp['mac'] # пока не используется

    data = {
        "success": False,
        "response": None,
        "error": None
    }

    logger.info(f"[Consumer id:{worker_id}] Проверяю наличие IP {ip} в ipset")
    try:
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset list | grep -w %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
    except Exception as e:
        logger.error(f"[Consumer id:{worker_id}] Не удалось выполнить команду '/sbin/ipset list'. Error: {e}")
        data["error"] = {"code": 3001, "msg": f"The command could not be executed '/sbin/ipset list'. Error: {e}"}
        return 1, json.dumps(data)

    logger.info(f"[Consumer id:{worker_id}] stdout={stdout}; stderr={stderr}")
    data["success"] = True

    # Если вывод есть(блокирован), возвращаем True, иначе выводим False
    if stdout.strip():
        logger.info(f"[Consumer id:{worker_id}] IP {ip} найден в ipset")
        data["response"] = True
        return 0, json.dumps(data)

    else:
        data["response"] = False
        logger.info(f"[Consumer id:{worker_id}] IP {ip} НЕ найден в ipset")
        return 0, json.dumps(data)


async def process_diagnostic(task_data, worker_id):
    """
    Функция прослойка, для распределения логики выполнения задач
    исходя из task задачи

    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param worker_id: ИД номер воркера, который выполняет задачу

    :return: возвращаем код результата и полезную нагрузку для ответа
    """
    task = json.loads(task_data)['task']
    match task:
        case 'is_blocked':
            return await is_blocked(task_data, worker_id)
        case 'get_arp':
            return await get_arp(task_data, worker_id)
        case 'config_public_ip':
            return await config_public_ip(task_data, worker_id)
        case 'unconfig_public_ip':
            return await unconfig_public_ip(task_data, worker_id)
        case _:
            logger.error(f"[Consumer id:{worker_id}] Некорректный запрос: отсутствует реализация текущей задачи: {task}")
            return 1, 1