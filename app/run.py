from app import server
if __name__ == '__main__':
    room = input('请输入要提醒的房间号: ')
    # server.execute(room)
    server.execute_ap(room)
