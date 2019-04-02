#!/usr/bin/env python3

name = 'XFFTS_logger'

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
        self.board_num = 2
        self.flag = ""
        self.log_flag = False
        pass

    def make_table(self):
        [self.op.make_table("BE{}".format(i), "(spectrum, time float)") 
                for i in range(1, self.board_num+1)]
        return

    def callback_spec(self, req, args):
        if self.log_flag:
            self.count += 1
            print(self.count)
            self.op.write("BE{}".format(args["index"]), "", (req.data, time.time()), cur_num=args["index"]-1, auto_commit=False)
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
                dbpath = '/home/exito/data/XFFTS_logger/{}.db'.format(t.strftime('%Y%m%d_%H%M%S'))
                self.op = Oplite(dbpath, self.board_num)
                self.make_table()
                print("DATABASE OPEN")
                self.log_flag = False
            else: pass
            
            if self.flag == "START":
                self.log_flag = True
            elif self.flag == "END":
                self.op.commit_data()
                self.op.close()
                print("DATABASE CLOSE")
                self.log_flag = False
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
        name = '/XFFTS_logger_flag',
        data_class = std_msgs.msg.String,
        callback = logg.callback_flag,
        queue_size = 1,
    )

    topic_from = [rospy.Subscriber(
                name = '/XFFTS_SPEC%d'%(i),
                data_class = std_msgs.msg.Float64MultiArray,
                callback = logg.callback_spec,
                callback_args = {'index': i },
                queue_size = 1,
            ) for i in range(1, logg.board_num+1)]

    rospy.spin()
