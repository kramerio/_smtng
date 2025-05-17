import time, re
from typing import List, Optional, Tuple, Union
from dataclasses import dataclass
import telnetlib

#todo кем-то написано, мной доколхожено(трошки)
# Вероятно много чего уже не используется и должно быть удалено
# но работает не трогай
@dataclass
class State:
    """ State response """
    port: int
    state: str
    link: str
    duplex: str
    speed: Optional[int]
    vlan_name: Optional[str]


@dataclass
class MacRecord:
    mac: str
    vlan_name: str
    vlan_id: int
    age: int
    flags: str


class Regex:
    """ Regex lines """
    STATE = ("(?P<port>{port}) +"
             "(?P<vlan_name>[a-zA-Z0-9)(]+)* +"
             "(?P<state>(E|D)) +"
             "(?P<link>(A|R|NP|L|D|d)) +"
             "(?P<speed>\\d+)* +"
             "(?P<duplex>[a-zA-Z]+)* *")
    INFO_DETAIL = ("Egress Port Rate:(\r\n)*(\t| )+"
                   "(?P<limit>(No-limit|(\\w+ +(Gbps|Kbps|Mbps))))")

    MAC = ("(?P<mac>[a-f0-9:]{17}) +"
           "(?P<vlan_name>[a-zA-Z0-9]+)"
           "\\("
           "(?P<vlan_id>\\d+)"
           "\\) +"
           "(?P<age>\\d+) +"
           "(?P<flags>[dspnmixlLMBbvPTDhoSr ]+) +"
           "%s")

    # d8:07:b6:7b:88:3b tmp_manage(1111) 0034 d m           3:8
    PORT_BY_MAC = ("%s +"
           "(?P<vlan_name>[a-zA-Z0-9]+)"
           "\\("
           "(?P<vlan_id>\\d+)"
           "\\) +"
           "(?P<age>\\d+) +"
           "(?P<flags>[dspnmixlLMBbvPTDhoSr ]+) +"
           "(?P<port>[0-9:]+)")

    VLANS = ("(?P<vlan_name>[a-zA-Z]+) +"
             "(?P<vlan_id>\\d+) *"
             "(-)+ ANY")


class Switch(object):
    TELNET_PORT = 3882

    class MODELS(object):
        X_350_24T = "X350-24t"
        X_350_48T = "X350-48t"
        X_450A_48T = "X450a-48t"
        X_440_48T = "X440-48t"
        X_450A_24X = "X450a-24x"

        SUPPORTED = [X_350_24T, X_350_48T, X_450A_48T, X_440_48T, X_450A_24X]

    class Responses(object):
        # GOOD
        SUCCESS = "successfully"
        DONE = "done"
        # BAD
        FAIL = "Fail"
        ERROR = "ERROR"
        ERROR2 = "Error"
        INVALID = "Invalid"

        ALL_ERRORS = [FAIL, ERROR, ERROR2, INVALID]

    class VLANS(object):
        INET = "INET"
        NAT = "NAT"
        FAKE = "fake"

    class CMD(object):
        ESC = "\x1B\x33"
        ESC2 = "\x9B"
        NEWLINE = "\n"
        LOGOUT = "logout"
        SAVE = "save"
        YES = "y"

        ACL_DYNAMIC_REMOVE_ACL = 'delete access-list "{acl_name}"'
        ACL_DYNAMIC_REMOVE_PORT = 'configure access-list del "{acl_name}" ports {port}'
        ACL_DYNAMIC_CREATE_PERMIT = 'create access-list {acl_name} "source-address {ip}/32;" "permit;"'
        ACL_DYNAMIC_CREATE_DENY = 'create access-list 0.0.0.0 "source-address 0.0.0.0/0;" "deny; count 0.0.0.0;"'
        ACL_DYNAMIC_ADD_PERMIT = 'configure access-list add "{acl_name}" first ports {port} ingress'
        ACL_DYNAMIC_ADD_DENY = 'configure access-list add "0.0.0.0" last ports {port} ingress'
        ACL_DYNAMIC_GET_LIST = 'show access-list dynamic'
        ACL_DYNAMIC_GET_PORT = 'show access-list port {port}'

        VLAN_ADD = 'configure vlan "{vlan_name}" add ports {port} untagged'
        VLAN_DEL = 'configure vlan "{vlan_name}" delete ports {port}'

        ENABLE = "enable ports {port}"
        DISABLE = "disable ports {port}"
        GET_PORT_STATE = "show ports {port} no-refresh"
        INFORMATION_DETAIL = "show ports {port} information detail"
        SHOW_FDB_PORTS = "show fdb ports {port}"
        SHOW_FDB_MAC = "show fdb {mac}"
        VLANS = "show vlan ports {port}"

class ExtremeTelnetClient(object):

    TIMEOUT = 15  # seconds
    TIMEOUT_ACL = 2 * 60  # 2 mins

    def __init__(self, model=None, ip=None, port=None,
                 login=None, password=None):

        # May this be updates to get model from the device after connection?
        self.model = model
        if self.model not in Switch.MODELS.SUPPORTED:
            raise Exception("Switch '{model}' isn't supported".format(
                model=self.model
            ))

        self.is_auth = False
        self._log_switch = bytes()
        self.t = telnetlib.Telnet()
        self.t.set_option_negotiation_callback(self.noop_option_callback)
        self.ip = ip
        self.port = port or Switch.TELNET_PORT

        self.login = login
        self.password = password

    def noop_option_callback(self, sock, command, option):
        # Ничего не отправляем — клиент не будет ничего лишнего слать.
        pass

    def get_log_switch(self):
        if self._log_switch:
            return self._log_switch.decode("utf-8", "ignore")\
                                   .replace("\\r", "\r").replace("\\n", "\n")
        return ''

    def clear_log_switch(self):
        self._log_switch = b''
        return True

    def connect(self):
        """ Connects to switches """
        self.t.open(self.ip, self.port, self.TIMEOUT)



    def auth(self):
        """ Does auth on switches """
        _try = 3
        is_success = False
        if self.login is None or self.password is None:
            return False
        try:
            while not is_success and _try > 0:
                result = self.t.expect(self.encode_message(
                    ["PassWord:", "Password:", "password:", "login:", "UserName:", "username:", "Username:"]),
                                       self.TIMEOUT)
                if result[0] >= 3:
                    _try -= 1
                    self.write(self.encode_message(self.login))
                    result = self.t.expect(self.encode_message(["PassWord:", "Password:", "password:", "#"]), self.TIMEOUT)
                    if result[0] < 0:
                        self.t.close()
                        return False
                    elif result[0] == 3:
                        self.is_auth = True
                        is_success = True
                    else:
                        self.write(self.password)
                        result = self.t.expect(self.encode_message(["#", "Authentication failed!", "Fail", "fail"]),
                                               self.TIMEOUT)
                        if result[0] == 0:
                            self.is_auth = True
                            is_success = True
                        else:
                            self.is_auth = False
                            is_success = False
                elif 3 > result[0] >= 0:
                    _try -= 1
                    self.write(self.password)
                    result = self.t.expect(self.encode_message(["#", "Authentication failed!", "Fail", "fail"]),
                                           self.TIMEOUT)
                    if result[0] == 0:
                        self.is_auth = True
                        is_success = True
                    else:
                        self.is_auth = False
                        is_success = False
                else:
                    self.t.close()
                    return False
                time.sleep(1)
            return is_success
        except:
            self.t.close()
            return False

    def write(self, command, new_line=True, log=False):
        """ writes commands to telnet """
        if not self.connected:
            raise EOFError('Telnet connection closed')

        # List of commands is an experimental feature !
        if isinstance(command, (list, tuple)):
            for cmd in command:
                self.write(cmd)
                if not self.wait_for([Switch.Responses.SUCCESS, '#'],
                              [Switch.Responses.FAIL]):
                    return False
        else:
            self.t.write(self.encode_message(command))
            if new_line:
                self.t.write(self.encode_message(Switch.CMD.NEWLINE))
            if log:
                self._log_switch += self.t.read_until(b"stupid_hack", 5)


    def wait_for(self, success, fails=None):
        """ Waits for special 'success' and 'fails' char sequences """
        if not isinstance(success, list):
            raise AttributeError("'success' has to be a list with keywords")
        if not isinstance(fails, (list, type(None))):
            raise AttributeError("'fails' has to be a list with keywords")

        is_success = False
        encoded_success = [self.encode_message(x) for x in success]
        encoded_fails = [self.encode_message(x) for x in (fails or [])]
        expected = encoded_success + encoded_fails

        result = self.t.expect(expected, self.TIMEOUT)

        if -1 < result[0] < len(encoded_success):
            is_success = True

        return is_success


    def execute_with_conditions(self, command: str,
                                conditions: List[List[str]],
            success: List[str], errors=None, timeout=TIMEOUT
    ) -> Tuple[bool, bytes]:
        """ Execute expecting conditions """
        encoded_success = [self.encode_message(x) for x in success]
        encoded_errors = [self.encode_message(x) for x in (errors or [])]
        encoded_conditions = [self.encode_message(x) for x, _ in conditions]
        expected = encoded_success + encoded_errors + encoded_conditions

        self.write(command)
        result = self.t.expect(expected, timeout)
        data = b""
        while True:
            data += result[2]
            success_len = len(encoded_success)
            errors_len = len(encoded_errors)
            conditions_len = len(encoded_conditions)

            success_range = (0, success_len)
            errors_range = (success_len, success_len + errors_len)
            conditions_range = (errors_range[1], success_len + errors_len +
                                conditions_len)

            result_id = result[0]
            if success_range[0] <= result_id < success_range[1]:
                return True, data
            elif errors_range[0] < result_id < errors_range[1]:
                return False, data
            else:
                condition, command = conditions[result_id - conditions_range[0]]
                self.write(command, False)
                result = self.t.expect(expected, timeout)

    def cable_diag(self, port):
        return False # заглушка

    def get_macs(self, port) -> List[str]:
        """ Get MAC addresses on the port """
        cmd = Switch.CMD.SHOW_FDB_PORTS.format(port=port)
        result, data = self.execute_with_conditions(cmd, [["Q", "Q"]], ["#"],
                                                    Switch.Responses.ALL_ERRORS)
        macs = []
        if result and data:
            r = re.compile(Regex.MAC % port)
            r_result = r.finditer(data.decode("utf-8"))
            for line in r_result:
                record = MacRecord(**line.groupdict())
                macs.append(record.mac.lower())
        return macs

    def get_speed(self, port) -> None | dict:
        """ Get TX/RX speed limit of the port """
        "show ports 2 information detail"
        cmd = Switch.CMD.INFORMATION_DETAIL.format(port=port)
        result, data = self.execute_with_conditions(cmd, [["SPACE", " "]],
                                                    ["#"],
                                                    Switch.Responses.ALL_ERRORS)
        if result and data:
            r = re.compile(Regex.INFO_DETAIL, re.IGNORECASE)
            r_result = r.search(data.decode("utf-8"))
            if r_result:
                speed = r_result.groupdict().get("limit", None)
                if speed is not None:
                    if speed.lower() == "no-limit":
                        speed = {"tx": "0",
                                 "rx": "0"}
                        return speed
                    speed = {"tx": speed,
                             "rx": speed}
                    return speed
            return None

    def expect_with_result(self, success, fails=None, timeout=TIMEOUT) -> Tuple[bool, bytes]:
        """ Expect success and error results """
        if not isinstance(success, list):
            raise AttributeError("'success' has to be a list with keywords")
        if not isinstance(fails, (list, type(None))):
            raise AttributeError("'fails' has to be a list with keywords")

        is_success = False
        encoded_success = [self.encode_message(x) for x in success]
        encoded_fails = [self.encode_message(x) for x in (fails or [])]
        expected = encoded_success + encoded_fails

        result = self.t.expect(expected, timeout)

        if -1 < result[0] < len(encoded_success):
            is_success = True
        return is_success, result[2]

    def execute_with_result(self, command, success, errors=None) -> Tuple[bool, bytes | None]:
        """ Execute with result """
        self.write(command)
        tx_result, response = self.expect_with_result(success, errors)

        if not tx_result:
            self.expect_with_result(['#'], None, self.TIMEOUT)
            return tx_result, None
        return True, response

    def get_vlans(self, port) -> list[list[str | bool]]:
        """ Get Vlans """
        cmd = Switch.CMD.VLANS.format(port=port)
        result, data = self.execute_with_result(cmd, ["#"],
                                                Switch.Responses.ALL_ERRORS)
        vlans = []
        if result and data:
            r = re.compile(Regex.VLANS, re.IGNORECASE)
            result_iter = r.finditer(data.decode("utf-8"))
            for item in result_iter:
                vlan = item.groupdict().get("vlan_name", None)
                if vlan is not None:
                    vlans.append([vlan, True])
        return vlans

    def get_state(self, port) -> Tuple[bool, bool]:
        """ Get the port state """
        cmd = Switch.CMD.GET_PORT_STATE.format(port=port)

        result, data = self.execute_with_result(cmd, ["Link State"],
                                                Switch.Responses.ALL_ERRORS)
        states = []
        if result and data:
            _, add_data = self.expect_with_result(["#"])
            complete_data = (data + add_data).decode("utf-8")
            r = re.compile(Regex.STATE.format(port=port), re.IGNORECASE)
            r_result = r.search(complete_data)
            if r_result:
                if State(**r_result.groupdict()).link == "A":
                    states.append(True)
                    # return "enabled"
                else:
                    states.append(False)
                    # return "disabled"
        return result, True in states

    def vlan_del(self, port, vlan_name):
        """ Remove a vlan of a port """
        cmd = Switch.CMD.VLAN_DEL.format(vlan_name=vlan_name, port=port)
        self.write(cmd)
        is_success, _data = self.expect_with_result(['#'], [Switch.Responses.INVALID], self.TIMEOUT) #self.expect(['#'], [Switch.Responses.INVALID])
        if not is_success:
            self.expect_with_result(['#'], None, self.TIMEOUT)
        return is_success

    def vlans_del(self, port):
        """ Remove vlans (INET, NAT, fake) of the port """
        inet_res = self.vlan_del(port, Switch.VLANS.INET)
        nat_res = self.vlan_del(port, Switch.VLANS.NAT)
        fake_res = self.vlan_del(port, Switch.VLANS.FAKE)
        return inet_res, nat_res, fake_res

    def vlan_add(self, port, vlan_name):
        """ Add a vlan for a port """
        cmd = Switch.CMD.VLAN_ADD.format(vlan_name=vlan_name, port=port)
        self.write(cmd)
        is_success = self.expect_with_result(['#'], [Switch.Responses.INVALID], self.TIMEOUT)
        if not is_success:
            self.expect_with_result(['#'], None, self.TIMEOUT)
        return is_success

    def port_clear(self, port):
        """
            Disable port for usage.
            We actually do not disable it we just remove vlans and clear ACLs
        """
        result_vlans = self.vlans_del(port)
        result_vlans = True if any(result_vlans) else False
        result_filter = self.ip_filter_del(port)
        return result_vlans and result_filter

    def save(self):
        """ Saves switch config """
        is_success = False
        self.write(Switch.CMD.SAVE)
        # TODO(s1z): Add switch list that require approving!
        result = self.t.expect([self.encode_message("(y/N)")], self.TIMEOUT)
        # Send Approve action if required
        if result[0] == 0:
            self.write(Switch.CMD.YES)
            result = self.t.expect([self.encode_message("#")], self.TIMEOUT)

        if result[0] > -1:  # Any success response -1. In out case it's only 0
            is_success = True

        return is_success


    def acl_dynamic_is_exist(self, name):
        try:
            cmd = Switch.CMD.ACL_DYNAMIC_GET_LIST
            pattern = r"^(?!\(\*\)hclag_arp)[\w\.\:]+(?=\s+Bound to \d+ interfaces)"
            data = b''
            self.write(self.encode_message(cmd))
            result = self.t.expect(self.encode_message(["<Q> to quit:", "#"]),
                                   self.TIMEOUT)

            if result[0] < 0:
                return False
            elif result[0] == 1:
                data = result[2]
            else:
                data = b'\n'.join(result[2].split(b'\n')[:-1])
                self.write(self.encode_message(" "))
                result = self.t.expect(self.encode_message(["#"]),
                                       self.TIMEOUT)
                data += result[2]

            #Переводим byte to str
            decoded_data = data.decode('utf-8')
            # Нормализуем строки
            normalized_data = "\n".join(line.strip() for line in decoded_data.splitlines() if line.strip())
            matches = re.findall(pattern, normalized_data, re.MULTILINE)
            # Если не задан поиск, то возвращаем просто массив acl
            if name is None:
                return matches
            # Проверяем наличие имени в массиве
            return name in matches
        except Exception as e:
            print("acl_dynamic_is_exist", e)
            return False

    def acl_dynamic_show_port(self, port):
        try:
            cmd = Switch.CMD.ACL_DYNAMIC_GET_PORT.format(port=port)
            pattern = r"^\s*\d+\s+([\w\.\-]+)\s+"


            result, data = self.execute_with_result(cmd, [" #"],
                                                    Switch.Responses.ALL_ERRORS)

            if result:
                normalized_data = "\n".join([line.strip() for line in data.decode('utf-8').splitlines() if line.strip()])
                matches = re.findall(pattern, normalized_data, re.MULTILINE)
                return matches
            return False
        except Exception as e:
            print("acl_dynamic_show_port", e)
            return  False

    def acl_dynamic_create_deny(self):
        try:

            cmd = Switch.CMD.ACL_DYNAMIC_CREATE_DENY
            result, data = self.execute_with_result(cmd, [" #"],
                                                    Switch.Responses.ALL_ERRORS)
            return result
        except Exception as e:
            print("acl_dynamic_create_deny", e)
            return False

    def acl_dynamic_create_permit(self, acl_name):

        try:
            if self.acl_dynamic_is_exist(acl_name):
                return True
            cmd = Switch.CMD.ACL_DYNAMIC_CREATE_PERMIT.format(acl_name=acl_name, ip=acl_name)
            result, data = self.execute_with_result(cmd, [" #"],
                                                    Switch.Responses.ALL_ERRORS)
            return result
        except Exception as e:
            print("acl_dynamic_create_permit", e)
            return False

    def acl_dynamic_add_permit(self, acl_name, port):
        try:
            if acl_name in self.acl_dynamic_show_port(port):
                return True
            if self.acl_dynamic_create_permit(acl_name):
                cmd = Switch.CMD.ACL_DYNAMIC_ADD_PERMIT.format(acl_name=acl_name, port=port)
                result, data = self.execute_with_result(cmd, ["done"],
                                                        Switch.Responses.ALL_ERRORS)
                self.expect_with_result(self.encode_message(['#']))

                return result
            return False
        except Exception as e:
            print("acl_dynamic_add_permit", e)
            return False

    def acl_dynamic_add_deny(self, port):
        try:
            if not self.acl_dynamic_is_exist('0.0.0.0'):
                if not self.acl_dynamic_create_deny():
                    return False
            cmd = Switch.CMD.ACL_DYNAMIC_ADD_DENY.format(port=port)
            result, data = self.execute_with_result(cmd, ["done"],
                                                    Switch.Responses.ALL_ERRORS)
            self.expect_with_result(self.encode_message(['#']))
            return result
        except Exception as e:
            print("acl_dynamic_add_deny", e)
            return False

    def acl_dynamic_remove_acl(self, name):
        try:
            cmd = Switch.CMD.ACL_DYNAMIC_REMOVE_ACL.format(acl_name=name)
            result, data = self.execute_with_result(cmd, [" #"],
                                                    Switch.Responses.ALL_ERRORS)
            return result
        except Exception as e:
            print("acl_dynamic_remove_acl", e)
            return False

    def ip_filter_del(self, port):
        try:
            acl_name = self.acl_dynamic_show_port(port)
            if "deny25" in acl_name:
                acl_name.remove("deny25")
            cmd = Switch.CMD.ACL_DYNAMIC_REMOVE_PORT
            for acl in acl_name[:]:
                result, data = self.execute_with_result(cmd.format(acl_name=acl, port=port), ["done"],
                                                        Switch.Responses.ALL_ERRORS)
                self.expect_with_result(self.encode_message(['#']))
                if result:
                    acl_name.remove(acl)
                if acl != '0.0.0.0':
                    self.acl_dynamic_remove_acl(acl)
            if not acl_name:
                return True, True
            elif "0.0.0.0" in acl_name and len(acl_name) == 1:
                return True, False
            elif len(acl_name) == 1:
                return False, True
            else:
                return False, False

        except Exception as e:
            print("ip_filter_del", e)
            return False, False

    def ip_filter_add(self, port, ip):
        try:
            return self.acl_dynamic_add_permit(ip, port), self.acl_dynamic_add_deny(port)
        except Exception as e:
            return False, False


    def disconnect(self):
        """ Disconnects from switch if connected """
        try:
            self.write(Switch.CMD.LOGOUT)
        except EOFError:
            pass
        self.t.close()
        self.is_auth = False
        return True

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