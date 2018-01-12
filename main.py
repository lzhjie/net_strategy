# coding: utf-8
# Copyright (C) zhongjie luo <l.zhjie@qq.com>
from load_conf import Conf
from ping_service import PingService, PingEvent
import pandas as pd
import numpy as np
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import thread
from thirdparty.Options import Options, Option
import sys


pd.set_option('display.max_columns', 0)
pd.set_option('display.width', 0)
pd.set_option('display.precision', 3)


def cost_func(m):
    return m.disconect * 100 + m.last_delay + m.d_price + m.lost


class MyPingEvent(PingEvent, BaseHTTPRequestHandler):
    def __init__(self, conf, alarm_delay=0.05):
        super(MyPingEvent, self).__init__(alarm_delay)
        self._extra_dtypes = {
            "last_delay": np.float32,
            "total_delay": np.float64,
            "delay_count": np.int32,
            "lost": np.int32,
            "timeouts": np.int32,
        }
        self._ping_times = 0
        self._cost_func = None
        self._httpd = None
        self._cost_str = conf.cost_lambda_str()
        self.__measure = None
        self.__conf = conf
        self.__measure_readonly = None
        self.__charset = conf.charset()

    def reset_measure(self):
        self._ping_times = 0
        for k, v in self._extra_dtypes.items():
            self.__measure[k] = v(0)

    def export(self, reset=True):
        temp = self.stat()
        if reset is True:
            self.reset_measure()
        temp.reset_index(inplace=True)
        temp.sort_values("cost", inplace=True)
        temp.drop_duplicates(["c_ip"], inplace=True)
        return temp

    def stat(self):
        # 多线程问题， http线程调用
        temp = self.__measure_readonly
        temp["ping_times"] = self._ping_times
        cost = temp.apply(self._cost_func, axis=1)
        temp["cost"] = cost.tolist()
        temp.charset = self.__charset
        return temp

    def healthy(self):
        temp = self.stat()
        temp = temp[(temp.total_delay==0) & (temp.lost==0) & (temp.timeouts==0)].copy()
        temp.sort_values("cost", inplace=True)
        temp.reset_index(inplace=True)
        return temp

    def unhealthy(self):
        temp = self.stat()
        temp = temp[(temp.total_delay>0) | (temp.lost>0) | (temp.timeouts>0)].copy()
        temp.sort_values("cost", inplace=True)
        temp.reset_index(inplace=True)
        return temp

    def e_delay(self, d_ip, c_ip, seq, pass_time, alarm_time):
        self.__measure.at[(c_ip, d_ip), "last_delay"] = pass_time
        self.__measure.at[(c_ip, d_ip), "delay_count"] += 1
        self.__measure.at[(c_ip, d_ip), "total_delay"] += pass_time
        # PingEvent.e_delay(self, d_ip, c_ip, seq, pass_time, alarm_time)

    def e_lost(self, d_ip, c_ip, seq, num):
        self.__measure.at[(c_ip, d_ip), "lost"] += num
        # PingEvent.e_lost(self, d_ip, c_ip, seq, num)

    def e_timeout(self, d_ip, c_ip, seq, pass_time):
        self.__measure.at[(c_ip, d_ip), "disconect"] += 1
        self.__measure.at[(c_ip, d_ip), "timeouts"] += 1
        # PingEvent.e_timeout(self, d_ip, c_ip, seq, pass_time)

    def e_recover(self, d_ip, c_ip, seq):
        self.__measure.at[(c_ip, d_ip), "disconect"] -= 1
        # PingEvent.e_recover(self, d_ip, c_ip, seq)

    def e_seqnotify(self, seq, time_):
        self.__measure_readonly = self.__measure.copy()
        self._ping_times += 1
        # PingEvent.e_seqnotify(self, seq, time_)

    def exit(self):
        temp = self.stat()
        temp.to_csv("ping_result.csv", float_format='%.3f', encoding=temp.charset)

    context = None

    # 在收到事件后初始化
    def init_in_subprocess(self):
        checkpoint = self.__conf.checkpoint()
        checkpoint.columns = ["c_" + i for i in checkpoint.columns]
        device = self.__conf.device()
        device.columns = ["d_" + i for i in device.columns]
        default_dev = "/".join(device["d_name"].values)
        checkpoint["c_device"] = checkpoint["c_device"].replace("*", default_dev)
        temp = checkpoint["c_device"].str.split("/", expand=True)
        temp = temp.stack().reset_index(level=0).set_index("level_0").rename(columns={0: "d_name"})
        temp = temp.join(checkpoint.drop("c_device", axis=1))
        measure = temp.join(device.set_index("d_name"), on=("d_name",))
        measure.dropna(subset=("d_ip",), inplace=True)
        measure["disconect"] = 0  # timeout +1, recover -1
        measure.set_index(["c_ip", "d_ip"], inplace=True)
        self.__measure = measure
        self.reset_measure()

        if self._cost_str:
            self._cost_func = eval(compile(self._cost_str, "", "eval"))
        else:
            self._cost_func = cost_func

        self._http = self.__conf.httpserver()
        server = HTTPServer(self._http, EventHttpHandler)
        def thread_func():
            while 1:
                try:
                    server.serve_forever()
                except (SystemExit, KeyboardInterrupt):
                    return
                except:
                    print sys.exc_info()[0]
        thread.start_new_thread(thread_func, tuple())
        print("start httpserver: %s %d" % self._http)
        MyPingEvent.context = self
        self._httpd = server
        self.__conf = None


class EventHttpHandler(BaseHTTPRequestHandler):
    """
    ['MessageClass', '__doc__', '__init__', '__module__', 'address_string', 
    'client_address', 'close_connection', 'command', 'connection', 
    'date_time_string', 'default_request_version', 'disable_nagle_algorithm', 
    'do_GET', 'end_headers', 'error_content_type', 'error_message_format', 
    'finish', 'funcs', 'handle', 'handle_one_request', 'headers', 
    'log_date_time_string', 'log_error', 'log_message', 'log_request', 
    'monthname', 'parse_request', 'path', 'protocol_version', 'raw_requestline', 
    'rbufsize', 'request', 'request_version', 'requestline', 'responses', 'rfile', 
    'send_error', 'send_header', 'send_response', 'server', 'server_version', 'setup', 
    'sys_version', 'timeout', 'types', 'version_string', 'wbufsize', 'weekdayname', 'wfile']
    """
    funcs = {
        "export": MyPingEvent.export,
        "stat": MyPingEvent.stat,
        "healthy": MyPingEvent.healthy,
        "unhealthy": MyPingEvent.unhealthy
    }
    types = {
        "html": lambda x: '<html>\n  <meta http-equiv="content-type" content="text/html; charset=utf-8"/>\n'+
                          pd.DataFrame.to_html(x, float_format='%.3f').encode("utf-8") + '</html>',
        "json": lambda x: pd.DataFrame.to_json(x, double_precision=3),
        "csv": lambda x: pd.DataFrame.to_csv(x, float_format='%.3f', encoding=x.charset),
    }
    surports = [".".join((x,y)) for x in funcs.keys() for y in types.keys()]
    surports_html = '<html>\n'\
                    '  <meta http-equiv="content-type" content="text/html; charset=utf-8"/>\n' \
                    '  <body>\n%s\n</body></html>'% \
                    ("<br/>\n".join(['<a href="%s">%s</a>' % (x, x) for x in surports]))
    def do_GET(self):
        if MyPingEvent.context is None:
            self.send_response(500)
            return
        temp = self.path.split(".")
        if len(temp) == 2:
            func, type = temp
            func = func.split("/")[-1]
            type = type.split("/")[0]
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(EventHttpHandler.surports_html)
            return
        if type == "ico":
            self.send_response(404)
            return
        funcs = EventHttpHandler.funcs
        types = EventHttpHandler.types
        f_func = funcs.get(func, None)
        f_type = types.get(type, None)
        if f_func is None or f_type is None:
            self.send_response(400, "func:%s type:%s" % (funcs.keys(), types.keys()))
            print func, type
            return
        self.send_response(200)
        self.send_header('Content-type', 'text/%s' % (type))
        self.end_headers()
        self.wfile.write(f_type(f_func(MyPingEvent.context)))
        return


if __name__ == "__main__":
    options = (
        Option("config", "c", "conf/conf.json"),
        Option("interval", "i", 1),
        Option("timeout", "t", 5),
    )
    options_ = Options(options)
    options_.parse_option(True)
    conf = Conf(options_.get("config"))
    pg = MyPingEvent(conf)
    device = conf.device()
    checkpoint = conf.checkpoint()
    s = PingService(device["ip"].values,
                    checkpoint["ip"].values,
                    pg,
                    checkpoint["alarmdelay"].values,
                    interval=options_.get("interval"),
                    timeout=options_.get("timeout"))
    s.start()
    del conf
    s.join()
