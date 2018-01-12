# coding: utf-8
# Copyright (C) zhongjie luo <l.zhjie@qq.com>
import pandas as pd
import numpy as np
import os
import json


def check_duplicate(df, col_name):
    temp = df.reset_index()
    result = temp.duplicated(subset=col_name, keep=False)
    index = np.argwhere(result).reshape(-1)
    return df.iloc[index]


class Conf:
    def device(self):
        """:return dataframe columns: name,ip,price"""
        return self._device.copy()

    def checkpoint(self):
        """:return dataframe columns: name,ip,device,alarmdelay"""
        return self._checkpoint.copy()

    def cost_lambda_str(self):
        """func params: device, measure, checkpoint"""
        return self._cost_lambda_str

    def interval(self):
        return self._interval

    def acktimeout(self):
        return self._acktimeout

    def httpserver(self):
        return tuple(self._httpserver)

    def charset(self):
        return self._charset

    def __init__(self, filename):
        self._httpserver = ["127.0.0.1", 8787]
        with open(filename, "r") as fp:
            conf = json.load(fp)
        conf_dir = os.path.dirname(filename)
        data_dir = conf.get("datadir", None)
        if data_dir is None or len(data_dir.strip()) == 0:
            data_dir = conf_dir
        # require fileds: name,ip,device,alarmdelay"
        self._charset = conf.get("charset", "utf-8")
        checkpoint = conf["checkpoint"]
        df = pd.read_csv(os.path.join(data_dir, checkpoint["file"]), encoding=self._charset)
        if len(df) == 0:
            print("checkpoint not fount")
            exit(100)
        dst, src = zip(*[(x.split(":") * 2)[:2] for x in checkpoint["field"].split(",")])
        df = df[list(src)]
        df.columns = dst
        temp = check_duplicate(df, "ip")
        if len(temp):
            print(temp)
            raise RuntimeError("checkpoint, duplicated ip")
        alarm_delay = max(conf["alarmdelay"], 0.001)
        df["alarmdelay"] = df["alarmdelay"].astype(np.float32).fillna(alarm_delay)
        self._checkpoint = df

        # require fileds: name,ip,price"
        device = conf["device"]
        df = pd.read_csv(os.path.join(data_dir, device["file"]), encoding=self._charset)
        if len(df) == 0:
            print("device not fount")
            exit(100)
        dst, src = zip(*[(x.split(":") * 2)[:2] for x in device["field"].split(",")])
        df = df[list(src)]
        df.columns = dst
        temp = check_duplicate(df, "name")
        if len(temp):
            print(temp)
            raise RuntimeError("device, duplicated name")
        temp = check_duplicate(df, "ip")
        if len(temp):
            print(temp)
            raise RuntimeError("device, duplicated ip")
        self._device = df
        self._cost = None
        cost = conf.get("costfunc", None)
        if cost is not None:
            self._cost_lambda_str = "lambda m:%s" % (cost["cost"])

        self._interval = max(conf["interval"], 1)
        self._acktimeout = max(conf["acktimeout"], 1)
        http = conf.get("httpserver")
        if http is not None:
            self._httpserver[0] = str(http.get("bindip", self._httpserver[0]))
            self._httpserver[1] = int(http.get("port", self._httpserver[1]))
