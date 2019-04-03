#!/usr/bin/env python3

name = 'sis_piv_logger'

import sys
import time
import datetime
import threading
sys.path.append("/home/exito/ros/src/opust_ros/lib")
from oplite import Oplite

import rospy
import std_msgs.msg


class logger(object):

    def __init__(self):
        self.count = 0
        self.sis_num = 4
        self.pm_num = 2
        self.flag = ""
        self.log_flag = False
        pass

    def make_table(self):
        [self.op.make_table("sis_cur_ch{}".format(i), "(curent, time float)")
                for i in range(1, self.sis_num+1)]

        [self.op.make_table("sis_vol_ch{}".format(i), "(voltage, time float)")
                for i in range(1, self.sis_num+1)]

        [self.op.make_table("pm_power_ch{}".format(i), "(power, time float)")
                for i in range(1, self.pm_num+1)]

        return

    def callback_cur(self, req, args):
        if self.log_flag:
            self.op.write("sis_cur_ch{}".format(args["index"]), "", (req.data, time.time()), cur_num=args["index"]-1, auto_commit=False)
        else: pass
        return

    def callback_vol(self, req, args):
        if self.log_flag:
            self.op.write("sis_vol_ch{}".format(args["index"]), "", (req.data, time.time()), cur_num=args["index"]-1, auto_commit=False)
        else: pass
        return

    def callback_power(self, req, args):
        if self.log_flag:
            self.op.write("pm_power_ch{}".format(args["index"]), "", (req.data, time.time()), cur_num=args["index"]-1, auto_commit=False)
        else: pass
        return

    def callback_flag(self, req):
        self.flag = req.data.upper()
        return

    def log(self):
        while not rospy.is_shutdown():
            while self.flag == "":
                time.sleep(0.001)
                continue

            if self.flag == "READY":
                t = datetime.datetime.fromtimestamp(time.time())
                dbpath = '/home/exito/data/sis_piv_logger/{}.db'.format(t.strftime('%Y%m%d_%H%M%S'))
                self.op = Oplite(dbpath, self.sis_num)
                self.op = Oplite(dbpath, self.pm_num)
                self.make_table()
                print("DATABASE OPEN")
                self.log_flag = False
            else: pass

            if self.flag == "START":
                self.log_flag = True
            elif self.flag == "END":
                self.log_flag = False
                self.op.commit_data()
                self.op.close()
                print("DATABASE CLOSE")
            else: pass

            self.flag = ""
        return

    def start_thread(self):
        th = threading.Thread(target=self.log)
        th.setDaemon(True)
        th.start()


if __name__ == '__main__':
    rospy.init_node(name)

    logg = logger()
    logg.start_thread()


    flag = rospy.Subscriber(
        name = '/sis_piv_logger_flag',
        data_class = std_msgs.msg.String,
        callback = logg.callback_flag,
        queue_size = 1,
    )

    topic_list_cur = [rospy.Subscriber(
                name = '/sis_cur_ch%d'%(i),
                data_class = std_msgs.msg.Float64,
                callback = logg.callback_cur,
                callback_args = {'index': i },
                queue_size = 1,
            ) for i in range(1, logg.sis_num+1)]

    topic_list_vol = [rospy.Subscriber(
                name = '/sis_vol_ch%d'%(i),
                data_class = std_msgs.msg.Float64,
                callback = logg.callback_vol,
                callback_args = {'index': i },
                queue_size = 1,
            ) for i in range(1, logg.sis_num+1)]

    topic_list_power = [rospy.Subscriber(
                name = '/pm_power_ch%d'%(i),
                data_class = std_msgs.msg.Float64,
                callback = logg.callback_power,
                callback_args = {'index': i },
                queue_size = 1,
            ) for i in range(1, logg.pm_num+1)]

    rospy.spin()
