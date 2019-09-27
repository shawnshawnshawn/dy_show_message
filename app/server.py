from apscheduler.schedulers.blocking import BlockingScheduler
import sched
import time
from datetime import datetime
import requests
import smtplib
from email.mime.text import MIMEText
from app import setting

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


def job(r_id):
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
            handle(res.json())
    except requests.ConnectionError as e:
        print("抓取信息失败: " + str(e))
    # 使用execute()时, 不注释
    # sc.enter(5, 0, job, (r_id,))


def handle(msg):
    # 注意 不加global, 这个方法内的of就会是局部变量,与全局变量of只是同名而已,他们完全是两个变量
    # 引用全局变量到方法并且改变变量的值时,一定要使用global
    global of
    r_info = msg.get('room')
    if r_info is not None:
        state = r_info.get("show_status")
        if state == 1:
            if of:
                # 发送邮件
                result = send_mail(r_info.get('room_url'))
                if result == 'success':
                    of = False
            else:
                print("已发过邮件不在发送!")
        else:
            of = True
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' 主播 ' + r_info.get('nickname') + ' 尚未开播!!!!')


# 使用sched作为定时器
def execute(room_id):
    sc.enter(0, 0, job, (room_id,))
    sc.run()


# 使用apscheduler作为定时器
def execute_ap(r_id):
    sd = BlockingScheduler()
    sd.add_job(job, 'interval', seconds=5, args=[r_id])
    sd.start()


def send_mail(l_url):
    # 局部定义全局变量
    global smtp
    mail = MIMEText(setting.msg + l_url)
    mail['Subject'] = setting.topic
    mail['Form'] = setting.sender
    mail['To'] = setting.recv
    try:
        smtp = smtplib.SMTP(setting.send_server, port=setting.server_port)
        # 下面这两个要加上,否则会报 'SMTP AUTH extension not supported by server' 错误
        smtp.ehlo()
        smtp.starttls()
        smtp.login(setting.sender, setting.s_pwd)
        smtp.sendmail(setting.sender, setting.recv, mail.as_string())
    except smtplib.SMTPException as e:
        print('发送邮件失败: ' + str(e))
        if e.args.__contains__(535):
            print('发送者邮箱账号或密码错误!')
        return 'fail'
    finally:
        smtp.close()
    print('邮件发送成功!!!')
    return 'success'
