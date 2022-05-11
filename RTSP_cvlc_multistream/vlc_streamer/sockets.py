__author__ = "Mikhail Novikov"
__email__ = "novikov.ms.in12@gmail.com"
__status__ = "Production"
__license__ = "GPLv3"


import socket

def chk_port(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((host, port))
    if result != 0:  # 0 - занят
        sock.close()
        return True
    else:
        sock.close()
        return False

