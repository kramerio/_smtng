import re
import paramiko
import logging

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


def get_arp(ip: str, vlan: str, mode: int, as_ip: str) -> bool | str:
    vlan = vlan if mode else 'fake'
    private_key_path = './shchapov_nat'
    key = paramiko.RSAKey(filename=private_key_path)
    b = paramiko.SSHClient()
    b.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    match vlan:
        case 'INET':
            try:
                b.connect(hostname=as_ip, username="shchapov", pkey=key, port=22, timeout=15,disabled_algorithms={'pubkeys': ['rsa-sha2-512', 'rsa-sha2-256']})
                logger.info(f"Подключился к core({as_ip}) для получение ARP-записи")
                stdin, stdout, stderr = b.exec_command("show ipa %s \r" % ip)
                result = stdout.read().decode('latin-1')
                b.close()
                r = re.compile("^.+%s +(?P<mac>([0-9,a-f]{2}(-|:)*){6}).+$" % ip, re.DOTALL | re.IGNORECASE)
                if re.match(r, result):
                    result = re.search(r, result.lower())
                    return result.groupdict()['mac']
            except Exception as e:
                logger.error(f"EXTREME:CORE:INET", exc_info=True)
                return 'Не удалось получить'
        case 'fake':
            try:
                b.connect(hostname="109.95.48.4", username="shchapov", pkey=key, port=13883, timeout=15)
                logger.info(f"Подключился к серверу (109.95.48.4) для получение ARP-записи")
                stdin, stdout, stderr = b.exec_command('/usr/sbin/arp -an | grep -w (%s)' % ip)
                result = stdout.read().decode('latin-1')
                b.close()
                r = re.compile("^.+(%s).+(?P<mac>([0-9,a-f]{2}(-|:)*){6}).+$" % ip, re.DOTALL | re.IGNORECASE)
                if re.match(r, result):
                    result = re.search(r, result.upper())
                    return result.groupdict()['mac']
            except Exception as e:
                logger.error(f"EXTREME:SERVER:fake:", exc_info=True)
                return 'Не удалось получить'
        case 'NAT':
            return False
        case _:
            return False
    return 'NO DATA'
