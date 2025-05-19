import telnetlib
import re
import time
from typing import List, Tuple, Union
import paramiko
#todo кем-то написано, мной доколхожено(трошки)
# Вероятно много чего уже не используется и должно быть удалено
# но работает не трогай
class Switch(object):
    TELNET_PORT = 23
    ENABLE = "enable"
    DISABLE = "disable"
    NO_LIMIT = "no_limit"

    class MODELS(object):
        D_LINK = "D_LINK"
        DES_3028 = "DES-3028"
        DES_3028P = "DES-3028P"
        DES_3052 = "DES-3052"
        DES_3200_10 = "DES-3200-10"
        DES_3200_18 = "DES-3200-18"
        DES_3200_26 = "DES-3200-26"
        DES_3200_28 = "DES-3200-28"
        DES_3200_52 = "DES-3200-52"
        DES_3526 = "DES-3526"
        DES_3550 = "DES-3550"
        DES_3552 = "DES-3552"
        DES_3828 = "DES-3828"
        DES_3828P = "DES-3828P"
        DGS_1100_06_ME = "DGS-1100-06/ME"
        DGS_1100_10_ME = "DGS-1100-10/ME"
        DGS_3200_10 = "DGS-3200-10"
        DGS_3200_16 = "DGS-3200-16"
        DGS_3100_24 = "DGS-3100-24"
        DGS_3100_24P = "DGS-3100-24P"
        DGS_3100_48 = "DGS-3100-48"
        DGS_3100_48P = "DGS-3100-48P"
        DGS_3100_24TG = "DGS-3100-24TG"
        DGS_3120_24SC = "DGS-3120-24SC"
        DGS_3120_24TC = "DGS-3120-24TC"
        DGS_3120_24PC = "DGS-3120-24PC"
        DGS_3120_48TC = "DGS-3120-48TC"
        DGS_3120_48PC = "DGS-3120-48PC"

        SUPPORTED = [DES_3028, DES_3028P, DES_3052, DES_3200_10, DES_3200_18, DES_3200_26,
                     DES_3200_28, DES_3200_52, DES_3526, DES_3550, DES_3552,
                     DES_3828, DGS_1100_06_ME, DGS_1100_10_ME, DGS_3200_10,
                     DGS_3200_16, DGS_3100_24, DGS_3100_24P, DGS_3100_48, DGS_3100_48P,
                     DGS_3100_24TG, DGS_3120_24SC, DGS_3120_24TC,
                     DGS_3120_24PC, DGS_3120_48TC, DES_3828P,D_LINK,
                     DGS_3120_48PC]

    class Responses(object):
        SUCCESS = "Success"
        FAIL = "Fail"

        ALL_ERRORS = [FAIL]

    class CMD(object):
        NEWLINE = "\n"
        LOGOUT = "logout"
        SAVE = "save"
        YES = "y"

    class VLANS(object):
        INET = "INET"
        NAT = "NAT"
        FAKE = "fake"


class DLinkTelnetClient(object):

    TIMEOUT = 15  # seconds
    def __init__(self, model=None, ip=None, port=None,
                 login=None, password=None):

        self.model = model
        self.mac = None

        self.is_auth = False
        self._log_switch = bytes()
        self.t = telnetlib.Telnet()
        self.t.set_option_negotiation_callback(self.noop_option_callback)
        self.ip = ip
        self.port = port or Switch.TELNET_PORT

        self.login = login
        self.password = password

    def noop_option_callback(self, sock, command, option):
        # Ничего не отправляем — клиент не будет отвечать на IAC.
        pass

    def get_device_model(self):
        result, data = self.execute_with_conditions(
            "show switch", [['ALL', 'a'], ['All', 'a']], ["#"],
            Switch.Responses.ALL_ERRORS
        )

        # \nDevice Type                       : DGS-1100-06/ME\r
        # \rDevice Type        : DES-3200-10 Fast Ethernet Switch\n
        data = data.decode("utf-8")
        r = re.compile(r"(\n|\r) *device.*: *(?P<model>[A-Z0-9/\-]{5,20}) *.*(\n|\r)",
                       re.IGNORECASE)
        for rr in re.finditer(r, data):
            self.model = rr.groupdict().get('model', None)
            if self.model is not None:
                break

        r = re.compile(r"(\n|\r) *mac.+: *(?P<mac>[a-fA-F0-9:\-]{17}) *(\n|\r)",
                       re.IGNORECASE)
        for rr in re.finditer(r, data):
            mac = rr.groupdict().get('mac', None)
            if mac is not None:
                self.mac = mac.lower()
                break

        if self.model not in Switch.MODELS.SUPPORTED:
            raise Exception("Switch '{model}' isn't supported".format(
                model=self.model
            ))



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
            # self.t.set_debuglevel(1)
            while not is_success and _try > 0:
                result = self.t.expect(self.encode_message(["PassWord:", "Password:", "password:", "login:", "UserName:", "username:", "Username:"]), self.TIMEOUT)
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
                        self.write(self.encode_message(self.password))
                        result = self.t.expect(self.encode_message(["#", "Authentication failed!", "Fail", "fail", "retry again"]), self.TIMEOUT)
                        if result[0] == 0:
                            self.is_auth = True
                            is_success = True
                        else:
                            self.is_auth = False
                            is_success = False
                elif 3 > result[0] >= 0:
                    _try -= 1
                    self.write(self.encode_message(self.password))
                    result = self.t.expect(self.encode_message(["#", "Authentication failed!", "Fail", "fail", "retry again"]), self.TIMEOUT)
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
            if self.model in [Switch.MODELS.DES_3526, Switch.MODELS.DES_3550, Switch.MODELS.DES_3552] and is_success:
                self.write('enable admin')
                _t = self.t.expect([b"#"], 5)
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
        diag = {}
        print(self.model)
        if self.model in [Switch.MODELS.DGS_1100_06_ME, Switch.MODELS.DGS_1100_10_ME]:
            self.write("cable diagnostic port %s" % port)
        elif self.model in [Switch.MODELS.DGS_3120_24PC, Switch.MODELS.DGS_3120_24SC, Switch.MODELS.DGS_3120_24TC, Switch.MODELS.DGS_3120_48TC, Switch.MODELS.DGS_3200_10]:
            self.write("cable_diag ports %s" % port)
        elif self.model.split("-")[0] == "DES":
            self.write("cable_diag ports %s" % port)
        else:
            return None

        result = self.t.read_until(b"#", 15)
        result=result.decode('latin-1')
        r = re.compile(
            rf"^.+{port} +"
            r"(?P<type>[0-9A-Za-z-_]+) +"
            r"(?P<lst>[A-Za-z]+ [A-Za-z]+) +"
            r"(?P<tres1>(OK|Unknown|No Cable|Shutdown|Pair *"
            r"(?P<pair1>[0-9]+) *"
            r"(?P<pair_s_1>(Short|Open|Ok)) +at +"
            r"(?P<meter1>[0-9]+) *M)) +"
            r"(?P<clen>(-|[0-9]+)) *"
            r"(\r*\n\r *"
            r"(?P<pairs>(Pair *(?P<pair>[0-9]+) *"
            r"(?P<pair_status>(Short|Open|OK|N/A)) +at +"
            r"(?P<meter>[0-9]+) *M\s*)+))*"
            r".+$",
            re.DOTALL | re.IGNORECASE
        )

        match = re.search(r, result.lower())
        if match:
            pairs = {}
            for p in re.finditer(
                    r"pair *(?P<pair>[0-9]+) *(?P<pair_status>(short|open|ok|n/a)) +at +(?P<meter>[0-9]+) *m",
                    result.lower()):
                pair_id = p.group("pair")
                pairs[pair_id] = (p.group("pair_status").lower(), p.group("meter"))

            diag = {
                "lst": match.group("lst"),
                "clen": match.group("clen"),
                "pair": pairs if pairs else None
            }
        if diag:
            return diag
        return None

    def get_macs(self, port):
        self.write("show fdb port %s" % port)
        r = re.compile("(\n\r|\r\n| )(?P<vid>[0-9]+) +"
                       "(?P<vname>[A-Za-z0-9]+) +"
                       "(?P<mac>([A-Fa-f0-9]{2}(-|:)*){6}) +(1:)*%s +"
                       "(?P<type>[A-Za-z]+) *"
                       "(\n\r|\r\n| +"
                       "(?P<status>[A-Za-z]+))*" % port, re.DOTALL | re.IGNORECASE)
        mac = []
        result = self.t.read_until(b"#", 5).decode('latin-1')
        for rr in re.finditer(r, result):
            mac.append(rr.groupdict()['mac'].replace("-", ":").lower())
        return mac

    def get_speed(self, port):
        speed = {}
        self.write("show bandwidth_control %s" % port)
        result = self.t.read_until(b"#", 5).decode('latin-1')
        r = re.compile("^.+%s +(?P<rx>([0-9]+|No( |_)Limit))+ +"
                       "(?P<tx>([0-9]+|No( |_)Limit))+"
                       ".+$" % port, re.DOTALL | re.IGNORECASE)

        if re.match(r, result):
            result = re.search(r, result.lower())
            rx = result.groupdict()['rx'].replace("_", " ")
            tx = result.groupdict()['tx'].replace("_", " ")
            speed = {"tx": tx if tx != "no limit" else "0",
                     "rx": rx if rx != "no limit" else "0"}
        return speed

    def get_vlans_all(self):
        vlans = {}
        self.write("show vlan")
        exp = self.t.expect([b"#", b"All", b"ALL"], 5)
        result = exp[2].decode('latin-1')
        if exp[0] > 0: self.write("a")
        result += self.t.read_until(b"#", 8).decode('latin-1')
        r = re.compile("VID +: +(?P<vid>[0-9]+) +VLAN NAME +: +(?P<vname>[A-Za-z0-9]+)(\n\r|\r\n)",
                       re.DOTALL | re.IGNORECASE)
        # hack
        result += self.t.read_until(b"#", 1).decode('latin-1')
        for rr in re.finditer(r, result):
            vid = rr.groupdict()['vid']
            vname = rr.groupdict()['vname']
            vlans[vid] = vname
        return vlans

    def get_state(self, port):
        cmd = f"show ports {port}"
        result, data = self.execute_with_conditions(
            cmd, [["Refresh", "q"]], ["#"],
            Switch.Responses.ALL_ERRORS
        )
        # \r 10   (F)  Enabled  Auto/Disabled   1000M/Full/None     Enabled           \n
        # \n\x1b[?25l2 Enabled  Auto/Disabled   Link Down           Enabled           \r
        # \r1:23 (C)   Enabled  Auto/Disabled   Link Down           Enabled  Disabled \r
        r = re.compile(r"(\r|\n).* *(1:)* *{port} *((\(C\)|\(F\))*) +"
                       "(?P<state>[A-Za-z]+) +"
                       "(?P<sett>[A-Za-z0-9/]+) +"
                       "(?P<conn>[A-Za-z0-9 /]+) +"
                       "(?P<adle>[A-Za-z/]+) *.*(\r|\n)".format(port=port))
        states = []
        for rr in re.finditer(r, data.decode("utf-8")):
            group_dict = rr.groupdict()
            states.append(True if group_dict["state"].lower() == 'enabled' else False)
        return result, any(states)

    def get_vlans(self, port):
        port_vlans = []
        vlans_all = self.get_vlans_all()
        self.write("show vlan ports %s" % port)
        result = self.t.read_until(b"#", 8).decode('latin-1')
        r = re.compile(
            r"(\n\r|\r\n) *(\d:)*(%s +)*(?P<vid>[0-9]+) +(?P<utg>(x|-)) +((?P<tg>(x|-)) +)*((?P<forb>(x|-)) +)*((?P<dyn>(x|-)) *)*" % port,
            re.DOTALL | re.IGNORECASE)
        for rr in re.finditer(r, result):
            vid = vlans_all.get(rr.groupdict()['vid'], rr.groupdict()['vid'])
            unt = rr.groupdict()['utg'].lower().find("x")
            port_vlans.append([vid, True if unt > -1 else False])
        return port_vlans

    def vlan_add(self, port, vlan_name):
        """ Add a vlan for a port """
        cmd = "config vlan {vlan_name} add untagged {port}"
        self.write(cmd.format(vlan_name=vlan_name, port=port))
        return self.wait_for([Switch.Responses.SUCCESS,
                              Switch.Responses.FAIL])

    def vlan_del(self, port, vlan_name):
        """ Removes a vlan of a port """
        cmd = "config vlan {vlan_name} delete {port}"
        self.write(cmd.format(vlan_name=vlan_name, port=port))

        return self.wait_for([Switch.Responses.SUCCESS,
                              Switch.Responses.FAIL])

    def vlans_del(self, port):
        """ Removes vlans (INET, NAT, fake) of the port """
        result_inet = self.vlan_del(port, Switch.VLANS.INET)
        result_nat = self.vlan_del(port, Switch.VLANS.NAT)
        result_fake = self.vlan_del(port, Switch.VLANS.FAKE)
        return result_inet, result_nat, result_fake

    def port_state(self, port, state):
        """ Manually sets port state """
        self.write("config ports {port} "
                     "state {state}".format(port=port, state=state))
        return self.wait_for_success()

    def port_on(self, raw_port):
        """ Turns port on """
        return self.port_state(raw_port, Switch.ENABLE)

    def port_off(self, raw_port):
        """ Turns port off """
        return self.port_state(raw_port, Switch.DISABLE)


    def wait_for_success(self, timeout=TIMEOUT):
        """ Waits for 'Success' keyword in char sequence """
        is_success = False
        result = self.t.expect([self.encode_message(Switch.Responses.SUCCESS),
                                self.encode_message(Switch.Responses.FAIL)],
                               timeout)
        if result[0] == 0:
            is_success = True
        return is_success

    def get_access_id(self, port: str | int) -> str:
        if not isinstance(port, str):
            port = str(port)
        # Если у нас порт 2:18, то у правила будет номер 218
        if ":" in port:
            port = port.replace(':', '')
            return port
        # иначе если это обычный порт 18, то номер будет 18
        else:
            return port

    def get_deny_access_id(self, port: int | str) -> str:
        if not isinstance(port, str):
            port = str(port)
        if self.model in [Switch.MODELS.DGS_3100_24,
                          Switch.MODELS.DGS_3100_24P,
                          Switch.MODELS.DGS_3100_48,
                          Switch.MODELS.DGS_3100_48P,
                          Switch.MODELS.DGS_3100_24TG,
                          Switch.MODELS.DGS_3120_24SC,
                          Switch.MODELS.DGS_3120_24TC,
                          Switch.MODELS.DGS_3120_24PC,
                          Switch.MODELS.DGS_3120_48TC]:
            # stack port syntax
            if ":" in port:
                stack_num, port_num = port.split(":")
                if stack_num != "1":
                    raise AttributeError("Invalid stack id: '{s_id}'!".format(
                        s_id=stack_num
                    ))
                return port.replace(":", '')
            # simple port syntax
            if len(port) < 3:
                return "1{port:0>2}".format(port=port)
            raise AttributeError("Invalid port: '{port}'!".format(port=port))
        elif self.model in [Switch.MODELS.DES_3028,
                            Switch.MODELS.DES_3028P,
                            Switch.MODELS.DES_3052,
                            Switch.MODELS.DES_3200_10,
                            Switch.MODELS.DES_3200_18,
                            Switch.MODELS.DES_3200_26,
                            Switch.MODELS.DES_3200_28,
                            Switch.MODELS.DES_3200_52,
                            Switch.MODELS.DES_3526,
                            Switch.MODELS.DES_3550,
                            Switch.MODELS.DES_3552,
                            Switch.MODELS.DES_3828,
                            Switch.MODELS.DES_3828P,
                            Switch.MODELS.DGS_1100_06_ME,
                            Switch.MODELS.DGS_1100_10_ME,
                            Switch.MODELS.DGS_3200_10,
                            Switch.MODELS.DGS_3200_16]:
            if ":" in port or len(port) > 2:
                raise AttributeError("DES switches don't support stack!")
            return port

    def ip_filter_add(self, port, ip):
        """ *** Adds IP filter to a port ***
            Example commands:
            config access_profile profile_id 2 add access_id 1 ip
                   source_ip X.X.X.X port 1 permit priority 7
            config access_profile profile_id 3 add access_id 1 ip
                   source_ip 0.0.0.0 port 1 deny
            (where X.X.X.X - client's IP)(1 - access list id and the port num)
        """
        if self.model in [Switch.MODELS.DGS_3100_24, Switch.MODELS.DGS_3100_24P,
                          Switch.MODELS.DGS_3100_48,
                          Switch.MODELS.DGS_3100_48P,
                          Switch.MODELS.DGS_3100_24TG]:
            self.write("config access_profile profile_id 2 "
                         "add access_id {port} "
                         "ip source_ip {ip} "
                         "ports {port} permit".format(port=port, ip=ip))
            profile_2 = self.wait_for(["Success"], ["Fail"])
            self.wait_for(['#'])
            # PAY ATTENTION!D-Link DGS-3120-24SC
            # access_id = 100 + "port".
            # For example port 15, access_id = 115, port 3, access_id = 103
            access_id = self.get_deny_access_id(port)
            self.write("config access_profile profile_id 3 "
                         "add access_id {access_id} "
                         "ip source_ip 0.0.0.0 "
                         "ports {port} deny".format(port=port,
                                                    access_id=access_id))
            profile_3 = self.wait_for(["Success"], ["Fail"])
            self.wait_for(['#'])
            return profile_2, profile_3
        elif self.model in [Switch.MODELS.DES_3028,
                            Switch.MODELS.DES_3028P,
                            Switch.MODELS.DES_3052,
                            Switch.MODELS.DES_3200_10,
                            Switch.MODELS.DES_3200_18,
                            Switch.MODELS.DES_3200_26,
                            Switch.MODELS.DES_3200_28,
                            Switch.MODELS.DES_3200_52,
                            Switch.MODELS.DES_3526,
                            Switch.MODELS.DES_3550,
                            Switch.MODELS.DES_3552,
                            Switch.MODELS.DES_3828,
                            Switch.MODELS.DES_3828P,
                            Switch.MODELS.DGS_3200_10,
                            Switch.MODELS.DGS_3200_16,
                            Switch.MODELS.DGS_3120_24SC,
                            Switch.MODELS.DGS_3120_24TC,
                            Switch.MODELS.DGS_3120_24PC,
                            Switch.MODELS.DGS_3120_48TC]:
            access_id = self.get_access_id(port)
            self.write("config access_profile profile_id 2 "
                         "add access_id {access_id} "
                         "ip source_ip {ip} "
                         "port {port} permit priority 7".format(port=port,
                                                                ip=ip, access_id=access_id))
            profile_2 = self.wait_for(["Success"],["Fail"])
            self.wait_for(['#'])
            self.write("config access_profile profile_id 3 "
                         "add access_id {access_id} "
                         "ip source_ip 0.0.0.0 "
                         "port {port} deny".format(port=port,access_id=access_id))
            profile_3 = self.wait_for(["Success"], ["Fail"])
            self.wait_for(['#'])
            return profile_2, profile_3
        elif self.model in [Switch.MODELS.DGS_1100_10_ME, Switch.MODELS.DGS_1100_06_ME]:
            self.write("config access_profile profile_id 2 "
                         "add access_id {port} "
                         "ip source_ip {ip} "
                         "port {port} permit".format(port=port, ip=ip))
            profile_2 = self.wait_for(["Success"], ["Fail"])
            self.wait_for(['#'])
            self.write("config access_profile profile_id 3 "
                         "add access_id {port} "
                         "ip source_ip 0.0.0.0 "
                         "port {port} deny".format(port=port))
            profile_3 = self.wait_for(["Success"], ["Fail"])
            self.wait_for(['#'])
            return profile_2, profile_3
        raise AttributeError("Switch '{model}' isn't supported".format(
            model=self.model
        ))

    def ip_filter_del(self, port):
        """
            *** Removes IP filter from the port ***
            Example commands:
            config access_profile profile_id 2 del access_id 1
            config access_profile profile_id 3 del access_id 1
        """
        cmd = "delete"
        if self.model in [Switch.MODELS.DGS_3100_24, Switch.MODELS.DGS_3100_24P,
                          Switch.MODELS.DGS_3100_48,
                          Switch.MODELS.DGS_3100_48P,
                          Switch.MODELS.DGS_3100_24TG]:
            self.write("config access_profile profile_id 2 "
                         "{cmd} access_id {port}".format(port=port, cmd=cmd))
            profile_2 = self.wait_for(["Success", "does not exist."], ["Fail"])
            self.wait_for(['#'])

            access_id = self.get_deny_access_id(port)
            self.write("config access_profile profile_id 3 {cmd} "
                         "access_id {access_id}".format(port=port, cmd=cmd,
                                                        access_id=access_id))
            profile_3 = self.wait_for(["Success", "does not exist."], ["Fail"])
            self.wait_for(['#'])
            return profile_2, profile_3
        elif self.model in [Switch.MODELS.DES_3028,
                            Switch.MODELS.DES_3028P,
                            Switch.MODELS.DES_3052,
                            Switch.MODELS.DES_3200_10,
                            Switch.MODELS.DES_3200_18,
                            Switch.MODELS.DES_3200_26,
                            Switch.MODELS.DES_3200_28,
                            Switch.MODELS.DES_3200_52,
                            Switch.MODELS.DES_3526,
                            Switch.MODELS.DES_3550,
                            Switch.MODELS.DES_3552,
                            Switch.MODELS.DES_3828,
                            Switch.MODELS.DES_3828P,
                            Switch.MODELS.DGS_1100_06_ME,
                            Switch.MODELS.DGS_1100_10_ME,
                            Switch.MODELS.DGS_3200_10,
                            Switch.MODELS.DGS_3200_16,
                            Switch.MODELS.DGS_3120_24SC,
                            Switch.MODELS.DGS_3120_24TC,
                            Switch.MODELS.DGS_3120_24PC,
                            Switch.MODELS.DGS_3120_48TC]:
            access_id = self.get_access_id(port)
            self.write("config access_profile profile_id 2 "
                         "{cmd} access_id {access_id}".format(access_id=access_id, cmd=cmd))
            profile_2 = self.wait_for(["Success", "does not exist."], ["Fail"])
            self.wait_for(['#'])
            self.write("config access_profile profile_id 3 "
                         "{cmd} access_id {access_id}".format(access_id=access_id, cmd=cmd))
            profile_3 = self.wait_for(["Success", "does not exist."], ["Fail"])
            self.wait_for(['#'])
            return profile_2, profile_3
        raise AttributeError("Switch '{model}' isn't supported".format(
            model=self.model
        ))

    def port_clear(self, port):
        """
            Disable port for usage.
            We actually do not disable it we just remove vlans and clear ACLs
        """
        result_vlans = self.vlans_del(port)
        result_vlans = True if any(result_vlans) else False
        result_filter = self.ip_filter_del(port)
        return result_vlans and result_filter


    def disconnect(self):
        """ Disconnects from switch if connected """
        try:
            self.write(Switch.CMD.LOGOUT)
        except EOFError:
            pass
        except BrokenPipeError:
            pass
        self.t.close()
        self.is_auth = False
        self.t.read_all()
        return True

    def save(self, wait_for_success=True):
        """ Saves switch config """
        is_success = False
        self.write(Switch.CMD.SAVE)
        # Some switches can be very slow with save command:
        # For example DES-3550 can save up to one minute
        timeout = self.TIMEOUT * 4 if wait_for_success else 0.1
        result = self.t.expect([self.encode_message("]..."),
                                self.encode_message("Success"),
                                self.encode_message("Done"),
                                self.encode_message("[OK]")], timeout)

        if result[0] == 0:
            self.write(Switch.CMD.YES)

        if result[0] > -1 or not wait_for_success:
            is_success = True

        if wait_for_success:
            self.t.read_until(self.encode_message('#'),
                                              self.TIMEOUT)
        else:
            self.t.read_eager()
        return is_success


    @property
    def connected(self):
        """ Checks if switch is connected """
        return self.t.sock and not self.t.eof

    @staticmethod
    def encode_message(message: str | list[str] | bytes) -> bytes | list[bytes]:
        """ Tries decode message to bytes """
        if isinstance(message, bytes):
            return message
        if isinstance(message, str):
            message = message.encode('utf-8', errors='ignore')
            return message
        if isinstance(message, list):
            encoded_message = [item.encode('utf-8', errors='ignore') for item in message]
            return encoded_message
        raise ValueError(f"Unexpected message type: {type(message)}. Expected str or list[str].")