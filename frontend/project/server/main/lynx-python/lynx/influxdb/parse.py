# -*- coding: utf-8 -*-
# queryの応答結果をパースする関数


import pandas as pd


def groupby_tags(dict):
    """ dictをデータフレームに変換する。

    TAGで集約した場合に、辞書形式で返却されたデータをデータフレームに変換。

    Args:
        dict(dict): GROUPBYで集約された辞書

    Returns:
        DataFrame: pandasのデータフレーム

    """
    df = pd.DataFrame()

    for k, v in dict.items():
        measurement, (tags) = k
        prefix = '{}_'.format(measurement)

        for tag in tags:
            name, value = tag
            prefix = prefix + '{}_{}_'.format(name, value)

        df = pd.concat([df, v.add_prefix(prefix)], axis=1)

    return df
