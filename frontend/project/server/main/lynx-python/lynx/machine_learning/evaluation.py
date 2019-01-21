# -*- coding: utf-8 -*-
# 学習モデルの推定結果を評価する

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics import classification_report


def output_classification_report(y_true, y_pred, labels=None, to_csv=None):
    """クラス分類レポートを出力

    Args:
        y_true(ndarray): 正解ラベル
        y_pred(ndarray): 推定ラベル
        labels(ndarray): 分類するラベル
        to_csv(str or None): csv保存パス（Noneの場合保存しない）

    """

    print(classification_report(y_true, y_pred, labels=labels, digits=3))

    if to_csv is not None:
        p, r, f1, s = precision_recall_fscore_support(y_true, y_pred)

        data = np.array([p, r, f1, s]).round(3).T
        headers = ["precision", "recall", "f1-score", "support"]
        df = pd.DataFrame(data, columns=headers)

        summary = pd.Series(data=[
                            np.average(p, weights=s),
                            np.average(r, weights=s),
                            np.average(f1, weights=s),
                            np.sum(s)],
                            index=headers)
        summary.name = "avg / total"

        df = df.append(summary)

        try:
            d = os.path.dirname(to_csv)
            os.makedirs(d, exist_ok=True)
        except FileExistsError:
            pass

        df.to_csv(to_csv)


def output_confusion_matrix(y_true, y_pred, labels=None, to_img=None):
    """混同行列を出力

    Args:
        y_true(ndarray): 正解ラベル
        y_pred(ndarray): 推定ラベル
        labels(ndarray): 分類するラベル
        to_img(str or None): 画像保存パス（Noneの場合保存しない）

    """
    cm = confusion_matrix(y_true, y_pred, labels)
    cm_normalized = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
    df = pd.DataFrame(cm_normalized)

    plt.figure()
    sns.heatmap(df, vmin=0.0, vmax=1.0, fmt="d", cmap='Blues',
               cbar=False, square=True, annot=cm, annot_kws={"fontsize": 18})
    plt.tight_layout()

    if to_img is not None:

        try:
            d = os.path.dirname(to_img)
            os.makedirs(d, exist_ok=True)
        except FileExistsError:
            pass

        plt.savefig(to_img, bbox_inches="tight", pad_inches=0.0)

    plt.show()
    plt.close()
