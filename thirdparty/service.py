# coding: utf-8
# Copyright (C) zhongjie luo <l.zhjie@qq.com>
from daemon import Daemon
import sys


class MyDaemon(Daemon):
    def __init__(self, func_, *args, **kwargs):
        super(MyDaemon, self).__init__(*args, **kwargs)
        self.func_ = func_
        sys.argv = sys.argv[:1] + sys.argv[2:]

    def run(self):
        self.func_()


def do_service(func):
    if sys.platform in set(["win32"]):
        print("%s unsupport!" % sys.platform)
        exit(-1)
    funcs = {
        "start": MyDaemon.start,
        "stop": MyDaemon.stop,
        "restart": MyDaemon.restart,
    }
    target = funcs.get(sys.argv[1], None) if len(sys.argv) > 1 else None
    if target is None:
        print("usage: %s %s [options]" % (sys.argv[0], "|".join(funcs.keys())))
        exit(1)

    service = MyDaemon(func, "net_strategy.pid")
    target(service)


if __name__ == "__main__":
    def test():
        print "test"


    do_service(test)
