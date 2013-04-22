#!/usr/bin/python
# coding:utf-8

import sys
import os
import logging
import MySQLdb
import time
import datetime
import MySQLdb.cursors
import MailSender
import subprocess
sys.path.append("/data/Walrus/lib")
import os_hdfs
import make_excel
import parse_argvs

logger = logging.getLogger()


class FeedBackWorker(object):

    """feedback class"""
    def __init__(self):
        super(FeedBackWorker, self).__init__()
        """Init.
        """
        self.db_ip = "localhost"
        self.db_user = "root"
        self.db_pwd = ""
        self.db_name = "walrus"
        self.db_conn = None
        self.db_cursor = None
        self.sleepTime = 10
        self.retryTime = 10
        self.connect_db()

    def connect_db(self):
        """Connect db.
        """
        # Wait until connect db.
        while True:
            try:
                self.db_conn = MySQLdb.connect(
                    self.db_ip, self.db_user, self.db_pwd, self.db_name, cursorclass=MySQLdb.cursors.DictCursor)
                self.db_conn.autocommit(1)
                self.db_cursor = self.db_conn.cursor()
                logger.info("connect db ip: %s, user: %s, pwd: %s, db_name: %s succeed." % (
                    self.db_ip, self.db_user, self.db_pwd, self.db_name))
                break
            except Exception, ex:
                logger.error("connect db ip: %s, user: %s, pwd: %s, db_name: %s error! (%s)" % (self.db_ip,
                             self.db_user, self.db_pwd, self.db_name, str(ex)))
                # Reconnect after 1min.
                time.sleep(60)

    def get_a_feedback_task(self):
        """Get a feedback task from db.
        """
        task_info = None
        sql = "select * from t_task where (f_status = 1 or f_status = 5 or f_status = 6) and (f_feedback_status is null or f_feedback_status = 0) order by f_time_start limit 1"
        try:
            self.db_cursor.execute(sql)
            data = self.db_cursor.fetchall()
            if not data:
                # logger.info("Find no task under feedback this time.")
                pass
            else:
                task_info = data[0]
        except Exception, ex:
            logger.error("Sql feedback task error: %s! (%s)" % (self.__sql_task, str(ex)))
            # try to reconnect
            self.connect_db()
        return task_info

    def update_feedback_status(self, f_task_id, f_feedback_status, f_hdfs_path='', f_nfs_path=''):
        """Update feedback status.

        @return:
                0 or null:	 任务未反馈
                1:           反馈失败
                2:	         任务已反馈（任务执行成功）
                3:	         任务已反馈（任务执行失败）
        """
        time_end = str(datetime.datetime.now())
        if f_feedback_status == 2:
            f_result_rows = 0
            f_result_size = 0
            try:
                f_result_rows = os_hdfs.hline("%s/part-*" % f_hdfs_path)
                f_result_size = os_hdfs.hdus("%s" % f_hdfs_path)
            except Exception, ex:
                logger.error("Get hdfs result line and size error: %s! (%s)" % (f_hdfs_path, str(ex)))
            sql = "update t_task set f_feedback_status=%s,f_result_rows=%d,f_result_size=%d,f_nfs_path='%s',f_time_end='%s' where f_task_id=%d" % (
                f_feedback_status, f_result_rows, f_result_size, f_nfs_path, time_end, f_task_id)
        else:
            sql = "update t_task set f_feedback_status=%s,f_time_end='%s' where f_task_id=%d" % (
                f_feedback_status, time_end, f_task_id)
        for i in xrange(self.retryTime):
            try:
                self.db_cursor.execute(sql)
                logger.info("%s succeed." % sql)
                break
            except Exception, ex:
                if i >= 2:
                    logger.error("%s error! (%s)" % (sql, str(ex)))
                # try to reconnect
                self.connect_db()
                time.sleep(self.sleepTime)

    def send_warn_mail(self, task_info):
        """Send warning mail.
        """
        f_task_id = task_info["f_task_id"]
        f_task_type = task_info["f_task_type"]
        f_status = task_info["f_status"]
        f_status_desc = task_info["f_status_desc"]
        f_task_author = task_info["f_task_author"]
        to = "xiumingzhu;tobywang;karlwu;yvanwang;acailiu;"
        if f_task_author is not None:
            cc = f_task_author
        else:
            cc = ""
        subject = "海象计算任务执行失败告警"
        d_status_info = {
            1: "任务校验失败",
            5: "任务计算失败"
        }
        f_status = d_status_info[f_status]
        body = ('''<html>
                                	<head>
                                	<style type="text/css">
                                	#warn_mail
                                	  {
                                	  font-family:"Microsoft YaHei";
                                	  width:100%%;
                                	  border-collapse:collapse;
                                	  }

                                	#warn_mail td, #warn_mail th
                                	  {
                                	  font-size:1em;
                                	  border:1px solid #98bf21;
                                	  padding:3px 7px 2px 7px;
                                	  }

                                	#warn_mail tr
                                	  {
                                	  font-size:1.1em;
                                	  text-align:left;
                                	  padding-top:5px;
                                	  padding-bottom:4px;
                                	  background-color:#A7C942;
                                	  color:#ffffff;
                                	  }

                                	#warn_mail tr.alt
                                	  {
                                	  color:#000000;
                                	  background-color:#EAF2D3;
                                	  }
                                	</style>
                                	</head><body>'''
                '''<p style="color:#A52A2A;font-weight:bold;">海象计算任务执行失败，请火速排查!</p>'''
                '''<table id="warn_mail">'''
                '''<tr><th>任务id</th><td>%(f_task_id)d</td></tr>'''
                '''<tr class="alt"><th>任务类型</th><td>%(f_task_type)s</td></tr>'''
                '''<tr><th>失败状态</th><td>%(f_status)s</td></tr>'''
                '''<tr class="alt"><th>失败详情</th><td>%(f_status_desc)s</td></tr>'''
                '''</table></body></html>'''
                % {"f_task_id": f_task_id,
                   "f_task_type": f_task_type,
                   "f_status": f_status,
                   "f_status_desc": f_status_desc})
        MailSender.send_mail(to, subject, body, htmlFlag=True, cc=cc)

    def send_zk_signal(self, zk_signal):
        """Send ZK signal.
        """
        if zk_signal:
            zk_cmd = "/usr/local/oms/zookeeper/ZKControl -t %s -a push" % zk_signal
            if subprocess.call(zk_cmd, stdout=subprocess.PIPE, shell=True):
                raise Exception, "Set ZK signal %s failed!" % zk_signal

    def send_mail(self, task_info, f_nfs_path):
        """Send response mail.
        """
        f_task_id = task_info["f_task_id"]
        f_task_type = task_info["f_task_type"]
        f_task_desc = task_info["f_task_desc"]
        f_task_author = task_info["f_task_author"]
        result_desc = task_info["f_result_schema"]
        bcc = "xiumingzhu;tobywang;karlwu;yvanwang;acailiu;"
        # cc = "acailiu;"
        if f_task_type == "video_accu_reach":
            f_nfs_path_src = os.path.dirname(f_nfs_path) + "/src_" + os.path.basename(f_nfs_path)
            os.rename(f_nfs_path, f_nfs_path_src)
            make_excel.make_add_uv_excel(result_desc, f_nfs_path_src, f_nfs_path)
        elif f_task_type == "video_freq":
            f_nfs_path_src = os.path.dirname(f_nfs_path) + "/src_" + os.path.basename(f_nfs_path)
            os.rename(f_nfs_path, f_nfs_path_src)
            make_excel.make_freq_excel(result_desc, f_nfs_path_src, f_nfs_path)
        elif f_task_type == "video_puv":
            f_nfs_path_src = os.path.dirname(f_nfs_path) + "/src_" + os.path.basename(f_nfs_path)
            os.rename(f_nfs_path, f_nfs_path_src)
            make_excel.make_puv_excel(result_desc, f_nfs_path_src, f_nfs_path)

        f_nfs_path = "http://10.137.129.174/result/walrus_%d.csv" % f_task_id
        if f_task_author is not None:
            to = f_task_author
        else:
            to = "walrus"
            logger.error("Task %d forgot his task submitter!" % f_task_id)
        subject = "海象计算任务反馈邮件"
        body = ('''<html>
                                    <head>
                                    <style type="text/css">
                                    body
                                      {
                                      font-family:"Microsoft YaHei";
                                      width:100%%;
                                      border-collapse:collapse;
                                      }
                                    </style>
                                    </head><body>'''
                '''Dear %(f_task_author)s,<br><br>'''
                ''' 您定制的海象任务，task_id: %(f_task_id)d [%(f_task_type)s] [%(f_task_desc)s] 已经顺利执行完成。<br><br>查看结果，请点击链接：<a href="%(f_nfs_path)s">结果文件</a>'''
                '''<br><br>祝好!<br>Walrus 项目组</body></html>'''
                % {"f_task_id": f_task_id,
                   "f_task_type": f_task_type,
                   "f_task_desc": f_task_desc,
                   "f_task_author": f_task_author,
                   "f_nfs_path": f_nfs_path})
        # MailSender.send_mail(to, subject, body, htmlFlag=True, cc=cc, attachments=f_nfs_path)
        MailSender.send_mail(to, subject, body, htmlFlag=True, bcc=bcc)

    def run(self):
        """Core function.
        """
        while True:
            # Get a feedback task from db.
            task_info = self.get_a_feedback_task()
            if task_info:
                try:
                    logger.info("Find a feedback task %d, now process it." % task_info["f_task_id"])
                    if task_info["f_status"] != 6:
                        # failed
                        self.send_warn_mail(task_info)
                        self.update_feedback_status(task_info["f_task_id"], 3)
                    else:
                        # success
                        if task_info["f_feedback"]:
                            fb_method = task_info["f_feedback"].split('_')
                        else:
                            fb_method = ["0"]
                            task_info["f_feedback"] = "0"
                        f_nfs_path = ""
                        if "0" in fb_method and "0" != task_info["f_feedback"]:
                            raise Exception, "Wrong feedback method %s!" % task_info["f_feedback"]
                        if "1" in fb_method or "2" in fb_method:
                            f_nfs_path = "/data/web_walrus/web_walrus/media/result/walrus_%d.csv" % task_info["f_task_id"]
                            # download result
                            os_hdfs.hdownload("%s/" % task_info["f_hdfs_path"], f_nfs_path)
                            if "1" in fb_method:
                                self.send_zk_signal(task_info["f_feedback_zk"])
                            if "2" in fb_method:
                                self.send_mail(task_info, f_nfs_path)
                            f_nfs_path = "http://10.137.129.174/result/walrus_%d.csv" % task_info["f_task_id"]
                        self.update_feedback_status(task_info[
                                                    "f_task_id"], 2, f_hdfs_path=task_info["f_hdfs_path"], f_nfs_path=f_nfs_path)

                except Exception, ex:
                    logger.error("Handle feedback task %d error! (%s)" % (task_info["f_task_id"], str(ex)))
                    self.update_feedback_status(task_info["f_task_id"], 1)
            else:
                time.sleep(self.sleepTime)

if __name__ == "__main__":
    parse_argvs.parse_argvs(sys.argv[1:], "WalrusFeedBack")

    logger.info("WalrusFeedBack service started.")
    feedback = FeedBackWorker()
    feedback.run()
