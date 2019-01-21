# -*- coding: utf-8 -*-

import unittest
import getpass
from lynx.influxdb.client import LynxClient


class TestClient(unittest.TestCase):
    host = 'ubi-lynx-db.naist.jp'
    port = 443
    username = input('Username: ')
    password = getpass.getpass()
    ssl = True
    verify_ssl = True
    start = '2018-08-13T08:00:00Z'
    end = '2018-08-14T08:00:00Z'


class TestLynxClient(TestClient):

    def setUp(self):
        self.client = LynxClient(host=self.host,
                            port=self.port,
                            username=self.username,
                            password=self.password,
                            ssl=self.ssl,
                            verify_ssl=self.verify_ssl)

    def test_positioning(self):
        self.client.positioning(self.start, self.end)

    def test_enocean_door(self):
        self.client.enocean_door(self.start, self.end)

    def test_enocean_motion(self):
        self.client.enocean_motion(self.start, self.end)

    def test_echonet_power_distribution_board_metering(self):
        self.client.echonet_power_distribution_board_metering(self.start, self.end)

    def test_echonet_operation_status(self):
        self.client.echonet_operation_status(self.start, self.end)

    def test_label(self):
        self.client.label(self.start, self.end)

    def test_label_list(self):
        self.client.label_list()

    def test_empatica(self):
        self.client.empatica('ibi', self.start, self.end)
