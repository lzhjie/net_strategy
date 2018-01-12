# coding: utf-8
# Copyright (C) zhongjie luo <l.zhjie@qq.com>
from thirdparty.ping import checksum, ICMP_ECHO_REQUEST
import sys
import struct
import ctypes
import socket
from multiprocessing import Process, Queue
import time
import thread
import select
import numpy as np


class PingEvent(object):
    timeout_id = 0
    delay_id = 1
    lost_id = 2
    recover_id = 3
    seq_notify_id = 4

    def __init__(self, alarm_delay=0.05):
        self._alarm_delay = alarm_delay
        pass

    def init_in_subprocess(self):
        raise NotImplementedError()

    def e_timeout(self, bind_ip, dest_ip, seq, pass_time):
        print("timeout, seq %d, %s->%s %.3fs"
              % (seq, bind_ip, dest_ip, pass_time))

    def e_delay(self, bind_ip, dest_ip, seq, pass_time, alarm_time):
        print("delay, seq %d, %s->%s %.3f(%.3f)s"
              % (seq, bind_ip, dest_ip, pass_time, alarm_time))

    def e_lost(self, bind_ip, dest_ip, seq, num):
        print("lost, seq %d, %s->%s, %d"
              % (seq, bind_ip, dest_ip, num))

    def e_recover(self, bind_ip, dest_ip, seq):
        print("recover, seq %d, %s->%s"
              % (seq, bind_ip, dest_ip))

    def e_seqnotify(self, seq, time_):
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("%s seqnotify, %d" % (t, seq))

    def exit(self):
        print "exit"


class PingService(Process):
    def __init__(self, bind_ips, dest_ips, ping_event,
                 dest_ips_delayalarms=None, interval=1.0, timeout=5.0):
        """
        type(bind_ips)->list, type(dest_ips)->list
        interval->seconds
        timeout->seconds
        """
        dest_ips_max_size = 65535 - 1  # ctypes.c_uint16(-1)， id=0不可用
        if len(dest_ips) > dest_ips_max_size:
            raise RuntimeError("dest_ips size overflow, %d > %d"
                               % (len(dest_ips), dest_ips_max_size))
        if not isinstance(ping_event, PingEvent):
            raise TypeError("require subclass of PingEvent")
        super(PingService, self).__init__()
        self._interval = max(interval, 1.0)
        self._bind_ips = bind_ips
        self._dest_ips = dest_ips
        self._dest_ips_delayalarms = dest_ips_delayalarms
        self.__sockets_map = {}  # id(socket)=>id
        self.__sockets = []
        self.__send_seq = ctypes.c_uint16(0)
        self.__recv_seqs = None
        self.__timeout = None
        self.__last_ping_time = 0
        self.__timeout_seq = max(1, (timeout + interval - 1) / interval)
        self.__event_obj = ping_event
        self.__event_q = Queue(2048)
        self.__event_service = EventService(self.__event_q, self.__event_obj)

    def start(self):
        super(PingService, self).start()
        self.__event_service.start()

    def join(self, timeout=None):
        self.__event_service.join(timeout)
        super(PingService, self).join()

    def terminate(self):
        self.__event_service.terminate()
        super(PingService, self).terminate()

    def run(self):
        self.__recv_seqs = np.zeros((len(self._dest_ips), len(self._bind_ips))).astype(np.uint16)
        self.__timeout = np.zeros(self.__recv_seqs.shape)
        self.__bind_ips_init()
        inv = self._interval
        thread.start_new_thread(self.__proc_response, tuple())
        ping_counter = 0
        start_time = default_timer()
        while 1:
            try:
                self.__check_ack_timeout()
                self.ping()
                ping_counter += 1
                pass_time = default_timer() - start_time
                sleep_time = inv * ping_counter - pass_time
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    print("warning, overload!!!, skip one round")
                    time.sleep(inv)
                    ping_counter = int(pass_time / inv)

            except (SystemExit, KeyboardInterrupt):
                return
            except:
                print(sys.exc_info()[0])
                time.sleep(inv)

    def __proc_response(self):
        inv = self._interval
        e_delay_time = self.__event_obj._alarm_delay
        delay_alarm = self._dest_ips_delayalarms
        if delay_alarm is None or len(delay_alarm) != len(self._dest_ips):
            delay_alarm = np.zeros(len(self._dest_ips))
            delay_alarm[:] = e_delay_time
        e_queue = self.__event_q
        eid_delay = PingEvent.delay_id
        eid_lost = PingEvent.lost_id
        eid_recover = PingEvent.recover_id
        r_seqs = self.__recv_seqs
        ignore_errors = set([11, 10035])
        timeout = self.__timeout
        dest_ips = self._dest_ips
        bind_ips = self._bind_ips
        while 1:
            whatReady = select.select(self.__sockets, [], [], inv)
            for item in whatReady[0]:
                try:
                    socket_id = self.__sockets_map[id(item)]
                    socket_ip = bind_ips[socket_id]
                    cur_time = default_timer()
                    pass_time = cur_time - self.__last_ping_time
                    while 1:
                        recPacket, addr = item.recvfrom(1024)
                        dest_ip = addr[0]
                        if recPacket:
                            type_, code, id_, seq = unpacket(recPacket)
                            id_ -= 1
                            if type_ != 0:
                                # 不可达
                                if type_ == 3:
                                    if timeout[id_, socket_id] == 0:
                                        e_queue.put((PingEvent.timeout_id,
                                                     (socket_ip, dest_ip, seq, pass_time)), False)
                                        timeout[id_, socket_id] = 1
                                continue
                            if dest_ips[id_] != dest_ip:
                                # 不是自己发出去包的响应
                                print "unkown ip", dest_ip
                                continue
                            r_seq = r_seqs[id_, socket_id]
                            if seq - r_seq > 1:
                                e_queue.put((eid_lost, (socket_ip, dest_ip, seq, seq - r_seq - 1)), False)
                                if timeout[id_, socket_id] == 1:
                                    e_queue.put((eid_recover, (socket_ip, dest_ip, seq)), False)
                                    timeout[id_, socket_id] = 0
                            r_seqs[id_, socket_id] = seq
                            r_pass_time = pass_time + (seq - r_seq - 1) * inv
                            # print("%s: %d bytes from %s: icmp_seq=%d time=%.3f s"
                            #         % (socket_ip, len(recPacket), dest_ip, seq, r_pass_time))
                            d_time = delay_alarm[id_]
                            if pass_time >= d_time:
                                e_queue.put((eid_delay, (socket_ip, dest_ip, seq, r_pass_time, d_time)), False)

                        else:
                            break
                except socket.error, (errno, msg):
                    if errno not in ignore_errors:
                        raise
                except (SystemExit, KeyboardInterrupt):
                    return
                except:
                    pass
            time.sleep(0)

    def __bind_ips_init(self):
        icmp = socket.getprotobyname("icmp")
        try:
            for i, ip in enumerate(self._bind_ips):
                mysocket = socket.socket(socket.AF_INET, socket.SOCK_RAW, icmp)
                mysocket.bind((ip, 0))
                mysocket.setblocking(False)
                self.__sockets_map[id(mysocket)] = i
                self.__sockets.append(mysocket)
        except socket.error, (errno, msg):
            if errno == 1:
                # Operation not permitted
                msg = msg + (
                    " - Note that ICMP messages can only be sent from processes"
                    " running as root."
                )
                raise socket.error(msg)
            raise  # raise the original error

    def __check_ack_timeout(self):
        s_seq = self.__send_seq.value
        if s_seq % self.__timeout_seq == 0 and s_seq > 0:
            e_queue = self.__event_q
            timeout = self.__timeout
            r_seqs = self.__recv_seqs
            timeouts = s_seq - r_seqs
            eid_time_out = PingEvent.timeout_id
            positions = np.argwhere(timeouts >= self.__timeout_seq)
            for i, j in positions:
                if timeout[i, j] == 0:
                    e_queue.put((eid_time_out,
                                 (self._bind_ips[j], self._dest_ips[i],
                                  r_seqs[i, j], timeouts[i, j] * self._interval)), False)
                    timeout[i, j] = 1

    def ping(self):
        cur_time = default_timer()
        self.__send_seq.value += 1
        s_seq = self.__send_seq.value
        self.__event_q.put((PingEvent.seq_notify_id, (s_seq, cur_time)), False)
        self.__last_ping_time = cur_time
        for id, ip in enumerate(self._dest_ips):
            for j, item in enumerate(self.__sockets):
                # id 为0时， 可能被修改， 8.8.8.8出现修改的情况
                p = packet(id + 1, s_seq)
                if item.sendto(p, (ip, 0)) != len(p):
                    time.sleep(0.01)
                    item.sendto(p, (ip, 0))
                # print "send ", ip, id, s_seq


class EventService(Process):
    def __init__(self, queue, ping_event):
        super(EventService, self).__init__()
        self.__queue = queue
        self.__ping_event = ping_event

    def run(self):
        queue = self.__queue
        ping_event = self.__ping_event
        events = [
            ping_event.e_timeout,
            ping_event.e_delay,
            ping_event.e_lost,
            ping_event.e_recover,
            ping_event.e_seqnotify,
        ]
        ping_event.init_in_subprocess()
        while 1:
            try:
                event_id, params = queue.get(True, 60)
                events[event_id](*params)
            except (SystemExit, KeyboardInterrupt):
                ping_event.exit()
                return
            except:
                pass


if sys.platform == "win32":
    # On Windows, the best timer is time.clock()
    f_time = time.clock
else:
    # On most other platforms the best timer is time.time()
    f_time = time.time


def default_timer():
    return float("%.3f" % f_time())


def packet(id, seq):
    my_checksum = 0
    id = ctypes.c_uint16(id).value
    seq = ctypes.c_uint16(seq).value
    header = struct.pack("!bbHHH", ICMP_ECHO_REQUEST, 0, my_checksum, id, seq)
    bytesInDouble = struct.calcsize("d")
    data_len = 56 - bytesInDouble
    data = data_len *"Q" # (192  - bytesInDouble) * "Q"
    data = struct.pack("d", default_timer()) + data
    my_checksum = checksum(header + data)
    header = struct.pack(
        "!bbHHH", ICMP_ECHO_REQUEST, 0, my_checksum, id, seq
    )
    packet = header + data
    return packet


def unpacket(packet):
    icmpHeader = packet[20:28]
    type, code, checksum, packetID, sequence = struct.unpack(
        "!bbHHH", icmpHeader
    )
    return type, code, packetID, sequence
