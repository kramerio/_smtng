import telnetlib
import time
import re
from typing import Union
import logging
logger = logging.getLogger("BDCOM_BAK_LIB")

#todo в ф-иях где нет """ """ нужно перепроверить их описать

def format_mac_address(mac: str) -> str | bool:
    """
    Преобразует MAC-адрес из формата "50ff.2044.0c1a" в формат "50:FF:20:44:0C:1A".

    :param mac: str, MAC-адрес в формате "50ff.2044.0c1a".
    :return: str, MAC-адрес в формате "50:FF:20:44:0C:1A".
    """
    if len(mac) != 14 or '.' not in mac:
        #raise ValueError("Некорректный формат MAC-адреса. Ожидаемый формат: 50ff.2044.0c1a")
        return False
    # Убираем точки и добавляем двоеточия
    mac = mac.replace('.', '')
    return ':'.join(mac[i:i+2] for i in range(0, len(mac), 2)).upper()

class Switch(object):
    TELNET_PORT = 23
    ENABLE = "enable"
    DISABLE = "disable"

    class MODELS(object):
        BDCOM = "BDCOM"
        GP3600_16B = "GP3600-16B"

        SUPPORTED = [GP3600_16B]

    class Responses(object):
        SUCCESS = "Success"
        FAIL = "fail"

        ALL_ERRORS = [FAIL]

    class CMD(object):
        NEWLINE = "\n"
        LOGOUT = "exit"
        SAVE = "write all"
        YES = "y"


class OltSwitch:
    TIMEOUT = 15  # seconds
    PROMPT = "Switch#"

    def __init__(self, model=None, ip=None, port=None,
                 login=None, password=None):

        self.model = model
        self.mac = None

        self.is_auth = False
        self._log = bytes()
        self._log_switch = bytes()
        self.t = telnetlib.Telnet()

        self.ip = ip
        self.port = port or Switch.TELNET_PORT

        self.login = login
        self.password = password



    def connect(self):
        try:
            self.t.open(self.ip, self.port, self.TIMEOUT)
            return True
        except Exception as e:
            logger.error(f"connect: Error - {e}")
            return False

    def auth(self):
        """ Does auth on switches """
        _try = 3
        if self.login is None or self.password is None:
            return False
        try:
            # цикл авторизации с 3 попытками
            while not self.is_auth and _try > 0:
                # ждем приглашение на ввод логина
                result = self.t.expect(self.encode_message(["login:", "UserName:", "username:", "Username:"]), self.TIMEOUT)
                logger.info(f"auth: raw={result[2]}")
                # Если приглашение есть:
                if result[0] >= 0:
                    _try -= 1
                    # Вводим логин и ждем приглашение на ввод пароля
                    self.write(self.login)
                    result = self.t.expect(self.encode_message(["PassWord:", "Password:", "password:", "#", ">"]), self.TIMEOUT)
                    logger.info(f"auth: raw={result[2]}")
                    # Если нет ничего(странно...) возвращаем ложь
                    if result[0] < 0:
                        return self.is_auth

                    # если после ввода логина нам прислали приветствие "#", ">", то попали на свитч без пароля
                    if result[0] in [3,4]:
                        self.is_auth = True

                    # Иначе если все таки приглашение на ввод пароля:
                    else:
                        # Вводим пароль и ждем приглашение "#", ">" в shell(ну или какую-то оболочку) свитча
                        self.write(self.password)
                        result = self.t.expect(self.encode_message(["#", ">", "Authentication failed!", "Fail", "fail"]), self.TIMEOUT)
                        logger.info(f"auth: raw={result[2]}")
                        # Если "#", ">", то зашли на свитч успешно
                        if result[0] in [0, 1]:
                            self.is_auth = True
                        else:
                            self.is_auth = False
                else:
                    # Если приветствия логина нет, считаем что попали на свитч без авторизации
                    self.is_auth = True

            # Если авторизовались настраиваем ебаный бдком олт чтоб нормально парсить вывод((
            # Почему так не сделать по умолчанию - загадка BDCOM инженеров
            if self.is_auth:
                self.write("enable")
                self.t.read_until(self.encode_message(self.PROMPT), self.TIMEOUT).decode("ascii", errors="ignore")
                self.write("config")
                self.write("terminal length 0")
                self.write("terminal width 0")
                self.write("exit")
                self.t.read_until(self.encode_message(self.PROMPT), self.TIMEOUT).decode("ascii", errors="ignore")

            return self.is_auth

        except Exception as e:
            logger.error(f"auth: Error - {e}")
            return self.is_auth

    def show_onu_information_sn(self, sn: str, fetch_many=False) -> bool | dict | list[dict]:
        """
        Вводит команду для получения информации об ONU через ее SN.
        Парсит ответ и возвращает dict, если запись найдена.
        Если записей нет либо больше чем одна при fetch_many=False - False.
        Если записей больше чем одна при fetch_many=True - list[dict]
        Поля словаря: IntfName, VendorID, ModelID, SN, LOID, Status, ConfigStatus, ActiveTime

        :param sn: серийный номер ONU

        :return: False or dict or list[dict]
        """
        cmd = f"show gpon onu-information sn {sn}"

        # отправляем команду
        self.write(cmd)
        raw = self.t.read_until(self.encode_message(self.PROMPT)).decode("ascii", errors="ignore")
        logger.info(f"show_onu_information_sn: raw: {raw}")
        # вычленяем табличную часть
        # Находим строку, которая состоит почти только из '-' и пробелов
        lines = raw.splitlines()
        try:
            sep_idx = next(
                i for i, ln in enumerate(lines)
                if re.match(r"^[\s-]+$", ln)
            )
        except StopIteration:
            logger.warning(f"show_onu_information_sn: table not found, return False")
            # таблицы нет вовсе
            return False

        # Находим первую строку с заголовками
        try:
            header_idx = next(
                i for i, ln in enumerate(lines)
                if "IntfName" in ln
            )
        except StopIteration:
            logger.warning(f"show_onu_information_sn: header not found, return False")
            return False

        # Находим первую строку с данными (она начинается с GPON0/)
        try:
            data_start = next(
                i for i, ln in enumerate(lines)
                if "GPON0/" in ln
            )
        except StopIteration:
            logger.warning(f"show_onu_information_sn: data not found, return False")
            return False

        # Получаем строку разделитель
        sep_line = lines[sep_idx]

        # Получаем сроку заголовков таблицы
        header_line = lines[header_idx]

        # Определяем диапазоны колонок по кол-ву '-' в sep_line
        col_ranges = [(m.start(), m.end()) for m in re.finditer(r"-+", sep_line)]
        # Парсим названия колонок, убираем пробелы в названии столбцов
        columns = [header_line[s:e].strip().replace(' ', '') for s, e in col_ranges]


        # Собираем строки данных до пустой строки или Switch#
        data_lines = []
        for ln in lines[data_start:]:
            text = ln.strip()
            if not text or text.startswith(self.PROMPT):
                break
            data_lines.append(ln)

        # На всякий еще одна проверка
        if not data_lines:
            logger.info(f"show_onu_information_sn: data not found, return False")
            return False

        logger.info(f"show_onu_information_sn: data lines={data_lines}")
        # Парсим каждую строку в словарь
        records = []
        for ln in data_lines:
            # ln = GPON0/2:52     HWTC      PU-X910      HWTC:1D80946A    N/A                      active   success       2025-04-26 22:52:15
            row = {}
            for idx, (s, e) in enumerate(col_ranges):
                # для последней колонки возьмём всё до конца строки
                if idx == len(col_ranges) - 1:
                    val = ln[s:].strip()
                else:
                    val = ln[s:e].strip()
                row[columns[idx]] = val

            records.append(row)

        # пост - обработка IntfName, превращаем GPON0/1:66 в [1, 66]
        for row in records:
            intf = row.get("IntfName")
            if intf:
                m = re.match(r"GPON0/(\d+):(\d+)", intf)
                if m:
                    row["IntfName"] = [int(m.group(1)), int(m.group(2))]

        logger.info(f"show_onu_information_sn: records={records}")
        # возвращаем результат
        if len(records) > 1:    # если записей больше одной
            if fetch_many:      # ожидаем ли мы несколько записей?
                return records  # да - возвращаем list[dict]
            return False

        return records[0]
    #{
    #"IntfName":     [2, 29],
    #"VendorID":     "HWTC",
    #"ModelID":      "PU-X910",
    #"SN":           "HWTC:1D80B640",
    #"LOID":         "N/A",
    #"Status":       "active",
    #"ConfigStatus": "success",
    #"ActiveTime":   "2025-01-17 15:32:14",
    #}

    def show_onu_information_interface(self, phy_port) -> list:
        """
        Вводит команду для получения информации об всех ONU на заданом интерфейсе.
        Парсит ответ и возвращает list[dict], если запис(и) найдена(ы).
        Если записей нет - list[].
        Поля словаря: IntfName, VendorID, ModelID, SN, LOID, Status, ConfigStatus, ActiveTime

        :param phy_port: физический номер порта на ОЛТ

        :return: [] or list[dict]
        """
        cmd = f"show gpon onu-information interface GPON0/{phy_port}"

        # отправляем команду
        self.write(cmd)

        raw = self.t.read_until(self.encode_message(self.PROMPT)).decode("ascii", errors="ignore")
        logger.info(f"show_onu_information_interface: raw={raw}")
        lines = raw.splitlines()

        try:
            sep_idx = next(
                i for i, ln in enumerate(lines)
                if re.match(r"^[\s-]+$", ln)
            )
        except StopIteration:
            logger.warning(f"show_onu_information_interface: table not found, return []")
            # таблицы нет вовсе
            return []

        # Находим первую строку с заголовками
        try:
            header_idx = next(
                i for i, ln in enumerate(lines)
                if "IntfName" in ln
            )
        except StopIteration:
            logger.warning(f"show_onu_information_interface: header not found, return []")
            return []

        # Находим первую строку с данными (она начинается с GPON0/)
        try:
            data_start = next(
                i for i, ln in enumerate(lines)
                if f"GPON0/{phy_port}:" in ln
            )
        except StopIteration:
            logger.warning(f"show_onu_information_interface: data not found, return []")
            return []

        # Получаем строку разделитель
        sep_line = lines[sep_idx]

        # Получаем сроку заголовков таблицы
        header_line = lines[header_idx]

        # Определяем диапазоны колонок по кол-ву '-' в sep_line
        col_ranges = [(m.start(), m.end()) for m in re.finditer(r"-+", sep_line)]
        # Парсим названия колонок, убираем пробелы в названии столбцов
        columns = [header_line[s:e].strip().replace(' ', '') for s, e in col_ranges]

        # Собираем строки данных до пустой строки или Switch#
        data_lines = []
        for ln in lines[data_start:]:
            text = ln.strip()
            if not text or text.startswith("Switch#"):
                break
            data_lines.append(ln)

        # На всякий еще одна проверка
        if not data_lines:
            logger.info(f"show_onu_information_interface: data not found, return []")
            return []

        # Парсим каждую строку в словарь
        records = []
        for ln in data_lines:
            # ln = GPON0/2:52     HWTC      PU-X910      HWTC:1D80946A    N/A                      active   success       2025-04-26 22:52:15
            row = {}
            for idx, (s, e) in enumerate(col_ranges):
                # для последней колонки возьмём всё до конца строки
                if idx == len(col_ranges) - 1:
                    val = ln[s:].strip()
                else:
                    val = ln[s:e].strip()
                row[columns[idx]] = val

            records.append(row)

        # пост - обработка IntfName, превращаем GPON0/1:66 в [1, 66]
        for row in records:
            intf = row.get("IntfName")
            if intf:
                m = re.match(r"GPON0/(\d+):(\d+)", intf)
                if m:
                    row["IntfName"] = [int(m.group(1)), int(m.group(2))]

        logger.info(f"show_onu_information_sn: records={records}")
        # возвращаем результат
        return records

    def show_lvl(self,phy_port,vport):
        try:
            command = f"show gpon interface GPON0/{phy_port}:{vport} onu optical-transceiver-diagnosis"
            self.write(command)
            time.sleep(1)
            output = self.t.read_until(self.encode_message(self.PROMPT)).decode("ascii", errors="ignore")
            logger.info(f"show_lvl: raw={output}")
            output = output.split(f"{phy_port}:{vport}")[2].split()
            if output:
                rx_lvl = output[3]
                tx_lvl = output[4]
                return float(rx_lvl), float(tx_lvl)
            return 0, 0
        except Exception as e:
            logger.error(f"show_lvl: ERROR - {e}")
            return 0, 0

    def show_mac(self,phy_port,vport):
        try:
            command = f"show mac address-table interface GPON0/{phy_port}:{vport}"
            self.write(command)
            output = self.t.read_until(self.encode_message(self.PROMPT)).decode("ascii", errors="ignore")
            logger.info(f"show_mac: raw={output}")
            output = output.split(f"{phy_port}:{vport}")[1].split("DYNAMIC")[0].split()[-1]
            res = format_mac_address(output)
            if res:
                return res

            return "NO DATA"
        except Exception as e:
            logger.error(f"show_mac: ERROR - {e}")
            return "NO DATA"


    def show_error(self,phy_port,vport):
        data = {
            "rx_sec": None,
            "tx_sec": None,
            "rx_total": None,
            "tx_total": None,
            "burst": None,
            "bip8": None,
            "lcdg": None,
            "rdi": None,
        }
        try:
            command = f"show interface gpoN0/{phy_port}:{vport}"

            received_packets_pattern = r"Received\s+(\d+)\s+packets"
            transmitted_packets_pattern = r"Transmitted\s+(\d+)\s+packets"
            input_packets_pattern = r"5 minutes input rate \d+ bits/sec, (\d+) packets/sec"
            output_packets_pattern = r"5 minutes output rate \d+ bits/sec, (\d+) packets/sec"
            errors_pattern = r"(\d+)\s+(unreceived burst|bip8 error|lcdg error|rdi error)"

            self.write(command)
            output = self.t.read_until(self.encode_message(self.PROMPT)).decode("ascii", errors="ignore")
            logger.info(f"show_error: raw={output}")
            errors = {match.group(2): int(match.group(1)) for match in re.finditer(errors_pattern, output)}

            data["rx_total"] = int(re.search(received_packets_pattern, output).group(1))
            data["tx_total"] = int(re.search(transmitted_packets_pattern, output).group(1))

            data["rx_sec"] = int(re.search(input_packets_pattern, output).group(1))
            data["tx_sec"] = int(re.search(output_packets_pattern, output).group(1))

            data["burst"] = errors.get("unreceived burst", 0)
            data["bip8"] = errors.get("bip8 error", 0)
            data["lcdg"] = errors.get("lcdg error", 0)
            data["rdi"] = errors.get("rdi error", 0)
            logger.info(f"show_error: data={data}")
            return data

        except Exception as e:
            logger.error(f"show_error: ERROR - {e}")
            return data

    def register_onu(self, vlan,phy_port,vport):
        command = f"interface GPON0/{phy_port}:{vport}"
        command1 = f"gpon onu flow-mapping-profile {vlan}"
        command2 = f"gpon onu uni 1 vlan-profile {vlan}"
        command3 = f"gpon onu uni 1 uni-profile MTU"
        cmd = [command, command1, command2, command3]
        _PROMPT = f"Switch_config_gpon0/{phy_port}:{vport}#"
        try:
            self.write("config")
            for c in cmd:
                self.write(c)
                _t = self.t.read_until(self.encode_message(_PROMPT)).decode("ascii", errors="ignore")
                logger.info(f"register_onu: raw={_t}")

            self.write(f"show running-config interface GPON0/{phy_port}:{vport}")
            _t = self.t.read_until(self.encode_message(_PROMPT)).decode("ascii", errors="ignore")
            logger.info(f"register_onu: raw={_t}")
            self.write("exit")
            self.write("exit")
            self.t.read_until(self.encode_message(self.PROMPT)).decode("ascii", errors="ignore")
            logger.info(f"register_onu: saving config...")
            if vlan in _t:
                self.write("write all")
                _t = self.t.read_until(self.encode_message(self.PROMPT), self.TIMEOUT*4).decode("ascii", errors="ignore")
                logger.info(f"register_onu: raw={_t}")
                return True
            else:
                return False

        except Exception as e:
            logger.error(f"register_onu: Error - {e}")
            return False

    def unregister_onu(self,sn,phy_port):
        #config
        #interface GPON0/x
        #no gpon bind-onu sn x
        command = f"interface GPON0/{phy_port}"
        command1 = f"no gpon bind-onu sn {sn}"
        cmd = [command, command1]
        _PROMPT = f"Switch_config_gpon0/{phy_port}#"
        try:
            self.write("config")
            for c in cmd:
                self.write(c)
                _t = self.t.read_until(self.encode_message(_PROMPT)).decode("ascii", errors="ignore")
                logger.info(f"unregister_onu: raw={_t}")

            self.write("exit")
            self.write("exit")
            self.t.read_until(self.encode_message(self.PROMPT)).decode("ascii", errors="ignore")
            logger.info(f"unregister_onu: saving config...")
            self.write("write all")
            _t = self.t.read_until(self.encode_message(self.PROMPT), self.TIMEOUT * 4).decode("ascii", errors="ignore")
            logger.info(f"unregister_onu: raw={_t}")
            return True

        except Exception as e:
            logger.error(f"unregister_onu: Error - {e}")
            return False

    def disconnect(self):
        """ Disconnects from switch if connected """
        try:
            # для выхода со свичта с режима Switch# нужно два раза ехит делать
            self.write(Switch.CMD.LOGOUT) # -> Switch>
            self.write(Switch.CMD.LOGOUT) # -> Connection closed
        except EOFError:
            pass
        except BrokenPipeError:
            pass
        self.t.close()
        self.is_auth = False
        self.t.read_all()
        return True

    def write(self, command, new_line=True):
        """ writes command to telnet """
        if not self.connected:
            raise EOFError("Telnet connection closed")

        self.t.write(self.encode_message(command))
        if new_line:
            self.t.write(self.encode_message(Switch.CMD.NEWLINE))

    @property
    def connected(self):
        """ Checks if switch is connected """
        return self.t.sock and not self.t.eof

    @staticmethod
    def encode_message(message: Union[str, list[str], bytes]) -> Union[bytes, list[bytes]]:
        """ Tries decode message to bytes """
        if isinstance(message, bytes):
            return message
        if isinstance(message, str):
            message = message.encode("utf-8")
            return message
        if isinstance(message, list):
            encoded_message = [item.encode("utf-8") for item in message]
            return encoded_message
        raise ValueError(f"Unexpected message type: {type(message)}. Expected str or list[str].")