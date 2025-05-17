import asyncio
import re
import json
from asyncio import subprocess
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

def add_log(condition: bool, success_msg: str, failure_msg: str) -> str:
    return success_msg if condition else failure_msg


# todo переработать  обработки except
async def unconfig_public_ip(task_data, id):
    """
    Через iptables удаляет правило NAT, которое давало абоненту уникальный внешний IP

    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param id: ИД номер воркера, который выполняет задачу

    :return: возвращаем код результата и полезную нагрузку для ответа
    """
    _tmp = json.loads(task_data)
    ip = _tmp['ip']
    public_ip = _tmp['public_ip']
    log = ''
    data = {
        "ip": ip,
        "public_ip": public_ip,
        "response": False,
        "log": ''
    }
    try:
        log = f"/Выполняю задачу \'unconfig_public_ip\' c IP {ip} -> {public_ip} на NAT-сервере (NSS)\n"
        logger.info(f"[Consumer id:{id}] Удаляю правило PREROUTING {public_ip} -> {ip}")

        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_DEL["PREROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{id}] Удаляю правило POSTROUTING {ip} -> {public_ip}")
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_DEL["POSTROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
        log += f"|Удаляю правила...(NSS)\n"
        logger.info(f"[Consumer id:{id}] Проверяю наличие правил {ip} в firewall")
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            f"/usr/sbin/iptables -vnL -t nat | grep {ip}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()

        # Если вывода нет(правила добавлены), возвращаем True, иначе выводим False
        if not stdout.strip():
            log += f"\Выполнения задачи успешно (NSS)\n"
            data['log'] = log
            data['response'] = True
            logger.info(f"[Consumer id:{id}] Успешно настроен NAT {ip} -> {public_ip}")
            return 0, json.dumps(data)
        else:
            log += f"\Выполнения задачи НЕ успешно (NSS)\n"
            data['log'] = log
            logger.info(f"[Consumer id:{id}] НЕ успешно настроен NAT {ip} -> {public_ip}")
            return 0, json.dumps(data)
    except Exception as e:
        log += f"\Выполнения задачи НЕ успешно (NSS)\n"
        data['log'] = log
        logger.error(f"[Consumer id:{id}]:unconfig_public_ip: Error - {e}", exc_info=True)
        return 1, json.dumps(data)


async def config_public_ip(task_data, id):
    """
    Фунцкия, которая через iptables настраивает правило NAT, для выхода в интернет с уникальным внешним IP
    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param id: ИД номер воркера, который выполняет задачу
    :return: возвращаем код результата и полезную нагрузку для ответа
    """
    # Перед добавлением нового правила, пробуем удалить старые, которые настроены на этот IP
    await unconfig_public_ip(task_data, id)

    _tmp = json.loads(task_data)
    ip = _tmp['ip']
    public_ip = _tmp['public_ip']
    log = ''
    data = {
        "ip": ip,
        "public_ip": public_ip,
        "response": False,
        "log": ''
    }
    try:
        log = f"/Выполняю задачу \'config_public_ip\' c IP {ip} -> {public_ip} на NAT-сервере (NSS)\n"
        logger.info(f"[Consumer id:{id}] Добавляю правило PREROUTING {public_ip} -> {ip}")

        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_ADD["PREROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
        logger.info(f"[Consumer id:{id}] Добавляю правило POSTROUTING {ip} -> {public_ip}")
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            PUBLIC_IP_ADD["POSTROUTING"].format(public_ip=public_ip, nat_ip=ip),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
        log += f"|Добавляю правила...(NSS)\n"
        logger.info(f"[Consumer id:{id}] Проверяю наличие правил {ip} в firewall")
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            f"/usr/sbin/iptables -vnL -t nat | grep {ip}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Получаем вывод команды
        stdout, stderr = await process.communicate()

        # Если вывод есть(правила добавлены), возвращаем True, иначе выводим False
        if stdout.strip():
            log += f"\Выполнения задачи успешно (NSS)\n"
            data['log'] = log
            data['response'] = True
            logger.info(f"[Consumer id:{id}] Успешно настроен NAT {ip} -> {public_ip}")
            return 0, json.dumps(data)
        else:
            log += f"\Выполнения задачи НЕ успешно (NSS)\n"
            data['log'] = log
            logger.info(f"[Consumer id:{id}] НЕ успешно настроен NAT {ip} -> {public_ip}")
            return 0, json.dumps(data)
    except Exception as e:
        log += f"\Выполнения задачи НЕ успешно (NSS)\n"
        data['log'] = log
        logger.error(f"[Consumer id:{id}]:config_public_ip: Error - {e}", exc_info=True)
        return 1, json.dumps(data)


async def get_arp(task_data, id):
    """
    Фунцкия, которая собирает arp записи по определенному IP
    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param id: ИД номер воркера, который выполняет задачу
    :return: возвращаем код результата и полезную нагрузку для ответа
    """
    _tmp = json.loads(task_data)
    ip = _tmp['ip']
    log = ''
    data = {
        "ip": ip,
        "response": False,
        "arp": 'NO DATA',
    }
    try:
        log = f"/Выполняю задачу \'get_arp\' c IP {ip} на NAT-сервере (NSS)\n"
        logger.info(f"[Consumer id:{id}] Проверяю наличие ARP записи с IP {ip}")
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            '/usr/sbin/arp -a -n | grep -w \(%s\)' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        log += f"|Собираю данные... (NSS)\n"
        # Получаем вывод команды
        stdout, stderr = await process.communicate()
        r = re.compile(r"^.+(%s).+(?P<mac>([0-9,a-f]{2}(-|:)*){6}).+$" % ip, re.DOTALL | re.IGNORECASE)
        result = stdout.decode().strip()
        log += f"\Выполнения задачи успешно (NSS)\n"
        data['log'] = log
        data['response'] = True
        # Поиск результата
        if re.match(r, result):
            logger.info(f"[Consumer id:{id}] ARP запись с IP {ip} найдена")
            result = re.search(r, result)
            data['arp'] = result.groupdict()['mac']
            return 0, json.dumps(data)
        else:
            logger.info(f"[Consumer id:{id}] ARP запись с IP {ip} НЕ найдена")
            return 0, json.dumps(data)

    except Exception as e:
        log += f"\Выполнения задачи НЕ успешно (NSS)\n"
        data['log'] = log
        logger.error(f"[Consumer id:{id}]:get_arp: Error - {e}", exc_info=True)
        return 1, json.dumps(data)


async def is_blocked(task_data, id):
    """
    Функция, которая проверяет нахождения определенного IP
    в blocklist на NAT-сервере
    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param id: ИД номер воркера, который выполняет задачу
    :return: возвращаем код результата и полезную нагрузку для ответа
    """
    _tmp = json.loads(task_data)
    ip = _tmp['ip']
    mac = _tmp['mac']
    log = ''
    data = {
        "ip": ip,
        "response": False,
        "log": '',
    }
    try:
        log = f"/Выполняю задачу \'is_blocked\' c IP {ip} на NAT-сервере (NSS)\n"
        logger.info(f"[Consumer id:{id}] Проверяю наличие IP {ip} в blocklist")
        # Выполняем команду на сервере
        process = await asyncio.create_subprocess_shell(
            '/sbin/ipset list | grep -w %s' % ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        log += f"|Собираю данные... (NSS)\n"
        # Получаем вывод команды
        stdout, stderr = await process.communicate()

        log += f"\Выполнения задачи успешно (NSS)\n"
        data['log'] = log
        # Если вывод есть(блокирован), возвращаем True, иначе выводим False
        if stdout.strip():
            logger.info(f"[Consumer id:{id}] IP {ip} найден в blocklist")
            data['response'] = True
            return 0, json.dumps(data)
        else:
            logger.info(f"[Consumer id:{id}] IP {ip} НЕ найден в blocklist")
            return 0, json.dumps(data)

    except Exception as e:
        log += f"\Выполнения задачи НЕ успешно (NSS)\n"
        data['log'] = log
        logger.error(f"[Consumer id:{id}] Error", exc_info=True)
        return 1, json.dumps(data)


async def process_diagnostic(task_data, id):
    """
    Функция прослойка, для распределения логики выполнения задач
    исходя из task задачи

    :param task_data: полезная нагрузка, данные которые нужно обработать
    :param id: ИД номер воркера, который выполняет задачу

    :return: возвращаем код результата и полезную нагрузку для ответа
    """
    task = json.loads(task_data)['task']
    match task:
        case 'is_blocked':
            return await is_blocked(task_data, id)
        case 'get_arp':
            return await get_arp(task_data, id)
        case 'config_public_ip':
            return await config_public_ip(task_data, id)
        case 'unconfig_public_ip':
            return await unconfig_public_ip(task_data, id)
        case _:
            logger.error(f"[Consumer id:{id}] Некорректный запрос: отсутствует реализация текущей задачи: {task}")
            return 1, 1