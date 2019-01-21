# -*- coding: utf-8 -*-

import pandas as pd
from influxdb import DataFrameClient
from influxdb.line_protocol import quote_literal, quote_ident

from .parse import groupby_tags


class LynxClient(DataFrameClient):

    """LynxのInfluxDBに接続するクライアント

    """

    def __init__(self, *args, **kwargs):
        super(LynxClient, self).__init__(*args, **kwargs)


    def positioning(self, start, end, duration=None):
        """超音波位置センサ

        超音波センサの平均値。
        値が取れていないところは前の値で補間する。

        Args:
            start (str): 対象時間の開始
            end (str): 対象時間の終了
            duration (str): 集約期間(influxdbのgroupbyルールに従う)

        Returns:
            DataFrame

        """

        measurement = 'positioning'
        if duration is None:
            text = "SELECT * FROM (SELECT x, y, z FROM {}) \
                    WHERE time > {} AND time < {}".format(quote_ident(measurement),
                                                          quote_literal(start),
                                                          quote_literal(end))
        else:
            text = "SELECT MEAN(*) FROM (SELECT x, y, z FROM {}) \
                    WHERE time > {} AND time < {} \
                    GROUP BY time({}) \
                    FILL(previous)".format(quote_ident(measurement),
                                           quote_literal(start),
                                           quote_literal(end),
                                           duration)
        data = self.query(text, database='lynx')
        df = data[measurement]

        return df


    def enocean_door(self, start, end, duration=None):
        """EnOceanドアセンサ

        EnOceanドアセンサの反応回数と最頻値。

        Args:
            start (str): 対象時間の開始
            end (str): 対象時間の終了
            duration (str): 集約期間(influxdbのgroupbyルールに従う)

        Returns:
            DataFrame

        """

        measurement = 'enocean-door'
        if duration is None:
            text = "SELECT * FROM (SELECT contact FROM {}) \
                    WHERE time > {} AND time < {} \
                    GROUP BY id".format(quote_ident(measurement),
                                        quote_literal(start),
                                        quote_literal(end))
        else:
            text = "SELECT COUNT(*), MODE(*) FROM (SELECT contact FROM {}) \
                    WHERE time > {} AND time < {} \
                    GROUP BY time({}), id \
                    FILL(0)".format(quote_ident(measurement),
                                    quote_literal(start),
                                    quote_literal(end),
                                    duration)
        data = self.query(text, database='lynx')
        df = groupby_tags(data)

        return df


    def enocean_motion(self, start, end, duration=None):
        """EnOcean人感センサ

        EnOcean人感センサの反応回数と最頻値。

        Args:
            start (str): 対象時間の開始
            end (str): 対象時間の終了
            duration (str): 集約期間(influxdbのgroupbyルールに従う)

        Returns:
            DataFrame

        """

        measurement = 'enocean-motion'
        if duration is None:
            text = "SELECT * FROM (SELECT pir FROM {}) \
                    WHERE time > {} AND time < {} \
                    GROUP BY id".format(quote_ident(measurement),
                                        quote_literal(start),
                                        quote_literal(end))
        else:
            text = "SELECT COUNT(*), MODE(*) FROM (SELECT pir FROM {}) \
                    WHERE time > {} AND time < {} \
                    GROUP BY time({}), id \
                    FILL(0)".format(quote_ident(measurement),
                                    quote_literal(start),
                                    quote_literal(end),
                                    duration)
        data = self.query(text, database='lynx')
        df = groupby_tags(data)

        return df


    def echonet_power_distribution_board_metering(self, start, end, duration=None):
        """ECHONETLite対応スマート分電盤

        ECHONETLite対応スマート分電盤による系統別消費電力の平均値。

        Args:
            start (str): 対象時間の開始
            end (str): 対象時間の終了
            duration (str): 集約期間(influxdbのgroupbyルールに従う)

        Returns:
            DataFrame

        """

        measurement = 'echonet-power_distribution_board_metering'
        if duration is None:
            text = "SELECT /ch.*/ FROM {} \
                    WHERE time > {} AND time < {}".format(quote_ident(measurement),
                                                          quote_literal(start),
                                                          quote_literal(end))
        else:
            text = "SELECT MEAN(/ch.*$/) FROM {} \
                    WHERE time > {} AND time < {} \
                    GROUP BY time({}) \
                    FILL(0)".format(quote_ident(measurement),
                                    quote_literal(start),
                                    quote_literal(end),
                                    duration)
        data = self.query(text, database='lynx')
        df = data[measurement]

        return df


    def echonet_operation_status(self, start, end, duration=None):
        """ECHONETLite対応家電の動作状態

        ECHONETLite対応家電の動作状態の最頻値。ON/OFFは1/0で表す。

        Args:
            start (str): 対象時間の開始
            end (str): 対象時間の終了
            duration (str): 集約期間(influxdbのgroupbyルールに従う)

        Returns:
            DataFrame

        """

        if duration is None:
            text = "SELECT * FROM (SELECT operation_status FROM /^echonet/) \
                    WHERE time > {} AND time < {} \
                    GROUP BY id".format(quote_literal(start),
                                        quote_literal(end))
        else:
            text = "SELECT MODE(*) FROM (SELECT operation_status FROM /^echonet/) \
                    WHERE time > {} AND time < {} \
                    GROUP BY time({}), id \
                    FILL(0)".format(quote_literal(start),
                                    quote_literal(end),
                                    duration)

        data = self.query(text, database='lynx')
        df = groupby_tags(data)

        return df.replace({True: 1, False: 0})


    def label(self, start, end):
        """行動ラベル

        被験者によってラベリングされた行動ラベルの生データ。

        Args:
            start (str): 対象時間の開始
            end (str): 対象時間の終了

        Returns:
            DataFrame

        """

        measurement = 'label'
        text = "SELECT * FROM {} \
                WHERE time > {} AND time < {} \
                ".format(quote_ident(measurement),
                         quote_literal(start),
                         quote_literal(end))

        data = self.query(text, database='app')
        df = data[measurement].groupby('user').apply(
            lambda df: df.pivot(columns='activity', values='status')
        ).reset_index('user').sort_index()

        return df


    def label_list(self):
        """行動ラベルの一覧

        行動ラベルの一覧。

        Returns:
            ndarray

        """

        text = "SHOW TAG VALUES WITH KEY = activity"
        data = self.query(text, database='app')
        df = pd.DataFrame(list(data.get_points()))

        return df.value.values


    def empatica(self, measurement, start, end, duration=None):
        """Empatica

        Empaticaのデータを取得する。

        Args:
            measurement (str): Empaticaのデータ種別。
                acc, bvp, eda, hr, ibi or temp.
            start (str): 対象時間の開始
            end (str): 対象時間の終了
            duration (str): 集約期間(influxdbのgroupbyルールに従う)

        Returns:
            DataFrame

        """

        if duration is None:
            text = "SELECT * FROM {} \
                    WHERE time > {} AND time < {} \
                    ".format(quote_ident(measurement),
                             quote_literal(start),
                             quote_literal(end))
        else:
            text = "SELECT MEAN(*) FROM {} \
                    WHERE time > {} AND time < {} \
                    GROUP BY time({}) \
                    FILL(0)".format(quote_ident(measurement),
                             quote_literal(start),
                             quote_literal(end),
                             duration)

        data = self.query(text, database='empatica')
        df = data[measurement]

        return df
