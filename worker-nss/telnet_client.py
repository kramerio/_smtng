from typing import Optional, Dict, Any, List, Union

from extreme import ExtremeTelnetClient, Switch as S_Switch
from dlink import DLinkTelnetClient, Switch as D_Switch
from bdcom_olt import OltSwitch, Switch as OLT_Switch

class TelnetClient:

    def __init__(self, model=None, ip=None, port=None,
                 login=None, password=None):
        self.model = model
        self.login = login
        self.password = password
        self.ip = ip
        self.port = port
        backend_lib = self._get_backend(self.model)
        self.backend = backend_lib(model, ip, port, login, password)


    def _get_backend(self, model):
        if model in D_Switch.MODELS.SUPPORTED:
            return DLinkTelnetClient
        elif model in S_Switch.MODELS.SUPPORTED:
            return ExtremeTelnetClient
        elif model in OLT_Switch.MODELS.SUPPORTED:
            return OltSwitch
        else:
            raise Exception("Switch '{model}' isn't supported".format(
                model=model
            ))

    # Ниже DLink & Extreme & BDCOM
    @property
    def connected(self):
        return self.backend.connected

    def connect(self):
        return self.backend.connect()

    def auth(self):
        return self.backend.auth()

    # Ниже только DLink & Extreme

    def get_macs(self, port):
        return self.backend.get_macs(port)

    def get_speed(self, port):
        return self.backend.get_speed(port)

    def get_vlans(self, port):
        return self.backend.get_vlans(port)

    def get_state(self, port):
        return self.backend.get_state(port)

    def cable_diag(self, port):  # (заглушка)
        return self.backend.cable_diag(port)

    def vlans_del(self, port):
        return self.backend.vlans_del(port)

    def vlan_add(self, port, vlan_name):
        return self.backend.vlan_add(port, vlan_name)

    def port_clear(self, port):
        return self.backend.port_clear(port)

    def ip_filter_add(self, port, ip):
        return self.backend.ip_filter_add(port, ip)

    def ip_filter_del(self, port):
        return self.backend.ip_filter_del(port)

    def disconnect(self):
        return self.backend.disconnect()

    def save(self):
        return self.backend.save()

    def get_log_switch(self):
        return self.backend.get_log_switch()

    def clear_log_switch(self):
        return self.backend.clear_log_switch()

    # Ниже GPON

    def show_onu_information_interface(self, phy_port):
        return self.backend.show_onu_information_interface(phy_port)

    def show_onu_information_sn(self, serial, fetch_many=False):
        return self.backend.show_onu_information_sn(serial, fetch_many)

    def unregister_onu(self, serial, phy_port):
        return self.backend.unregister_onu(serial, phy_port)

    def register_onu(self, vlan, phy_port, vport):
        return self.backend.register_onu(vlan, phy_port, vport)

    def show_error(self, phy_port, vport):
        return self.backend.show_error(phy_port, vport)

    def show_lvl(self, phy_port, vport):
        return self.backend.show_lvl(phy_port, vport)

    def show_mac(self, phy_port, vport):
        return self.backend.show_mac(phy_port, vport)
