from apscheduler.schedulers.blocking import BlockingScheduler
import sched
import time
from datetime import datetime
import requests
import smtplib
from email.mime.text import MIMEText
import logging
import pymysql as pymysql
from redis import StrictRedis

send_server = 'smtp.office365.com'
# 邮件服务器端口
server_port = 587
# 发送邮箱
sender = 'jiangliuer_shawn@outlook.com'
# 发送邮箱密码
s_pwd = '*************'

# 发送内容
msg = '您订阅的主播 %s 开播了!!!!!!!!!直播地址: '
# 邮件主题
topic = '斗鱼主播开播提醒'

# 创建SQL连接
sql_conn = pymysql.connect(host='127.0.0.1', user='root', password='root', database='personal', charset='utf8mb4')
# 获取光标(用于执行SQL语句)
cursor = sql_conn.cursor()

# redis连接
redis = StrictRedis(host='127.0.0.1', port=6379, db=0, password='root123')

# 调度器
sc = sched.scheduler(time.time, time.sleep)

# 爬取地址
host = 'https://www.douyu.com'
base_url = host + '/betard/'
# 这里的user_agent是斗鱼接口请求中找的
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/77.0.3865.90 Safari/537.36'

# 发送邮件开关, 保证每次开播只发送一次邮件
of = True

# 日志
logger = logging.getLogger()


def job():
    rooms = queryRoom()
    for room in rooms:
        print("房间号: %s 启动" % room[1])
        r_id = room[1]
        # 注意, 不同网站headers不同. 请求头错误,请求会报400
        headers = {
            'authority': 'www.douyu.com',
            'referer': 'https://www.douyu.com/betard/%s' % r_id,
            'user-agent': user_agent
        }
        url = base_url + str(r_id)
        try:
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                handle(res.json(), r_id)
        except requests.ConnectionError as e:
            print("抓取信息失败: " + str(e))
    # 使用execute()时, 不注释
    # sc.enter(5, 0, job, (r_id,))


def handle(msg, r_id):
    # 注意 不加global, 这个方法内的of就会是局部变量,与全局变量of只是同名而已,他们完全是两个变量
    # 引用全局变量到方法并且改变变量的值时,一定要使用global
    retry = 0
    r_info = msg.get('room')
    if r_info is not None:
        state = r_info.get("show_status")
        subs = queryReceive(r_id)
        if state == 1:
            if len(subs) == 0:
                logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' 主播 ' + r_info.get('nickname') + ' 无人订阅!!!!')
                return
            for sub in subs:
                if redis.get('personal.email.room.send.%s.%s' % (
                        sub[1], sub[2])) is None:
                    # 发送邮件
                    print("id=%s,room_id=%s,sub_email=%s,is_sub=%s,create_time=%s" % (
                        sub[0], sub[1], sub[2], sub[3], sub[4]))
                    try:
                        while retry < 3:
                            result = send_mail(r_info.get('room_url'), sub[2], r_info.get('nickname'))
                            retry = retry + 1
                            if result == "success":
                                retry = 0
                                break
                        redis.set('personal.email.room.send.%s.%s.%s' % (
                            sub[1], sub[2], datetime.now().strftime('%Y-%m-%d')), 'DAY')
                    except Exception as e:
                        logger.info(e)
                else:
                    # 不发送邮件
                    print("房间 %s 开播,邮箱为 %s 的用户已发过邮件不在发送!" % (r_id, sub[2]))
        else:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' 主播 ' + r_info.get('nickname') + ' 尚未开播!!!!')
            for sub in subs:
                redis.delete(
                    'personal.email.room.send.%s.%s.%s' % (sub[1], sub[2], datetime.now().strftime('%Y-%m-%d')))


# 使用sched作为定时器
def execute(room_id):
    sc.enter(0, 0, job, (room_id,))
    sc.run()


# 使用apscheduler作为定时器
def execute_ap():
    sd = BlockingScheduler()
    sd.add_job(job, 'interval', seconds=5)
    sd.start()


def send_mail(l_url, recv, n_name):
    # 局部定义全局变量
    global smtp
    mail = MIMEText(msg % n_name + l_url)
    mail['Subject'] = topic
    mail['Form'] = sender
    mail['To'] = recv
    try:
        smtp = smtplib.SMTP(send_server, port=server_port)
        # 下面这两个要加上,否则会报 'SMTP AUTH extension not supported by server' 错误
        smtp.ehlo()
        smtp.starttls()
        smtp.login(sender, s_pwd)
        smtp.sendmail(sender, recv, mail.as_string())
    except smtplib.SMTPException as e:
        print('发送邮件失败: ' + str(e))
        if e.args.__contains__(535):
            print('发送者邮箱账号或密码错误!')
        return 'fail'
    finally:
        smtp.close()
    print('邮件发送成功!!!')
    return 'success'


def queryReceive(r_id):
    sql = "select * from tb_sub where is_sub = 1 and room_id = %s" % r_id
    try:
        # 检查SQL连接是否关闭,关闭重连
        sql_conn.ping(reconnect=True)
        # 游标执行SQL语句
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as e:
        logger.info('查询mysql错误: ' + str(e))
        sql_conn.rollback()
    finally:
        sql_conn.close()


def queryRoom():
    sql = "select * from tb_room where is_delete = 0"
    try:
        # 检查SQL连接是否关闭,关闭重连
        sql_conn.ping(reconnect=True)
        # 游标执行SQL语句
        cursor.execute(sql)
        return cursor.fetchall()
    except Exception as e:
        logger.info('查询mysql错误: ' + str(e))
        sql_conn.rollback()
    finally:
        sql_conn.close()


if __name__ == '__main__':
    execute_ap()
