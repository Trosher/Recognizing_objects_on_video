from init.init_docker import init_docker
from loguru import logger
from os import getcwd
from datetime import datetime
from sys import platform

def check_system_win():
    return True if platform == "win32" or platform == "win64" else False

def get_time():
    time = datetime.now()
    return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

@logger.catch
def create_log_file() -> str:
    slash = "\\" if check_system_win() else "/"
    path_to_log = f"{getcwd()}{slash}logs{slash}build_logs{slash}runtime_build_{get_time()}.log"
    logger.add(path_to_log, retention="1 days")
    return path_to_log, slash

def build():
    path_to_log, slash = create_log_file()
    logger.info("Start work build function.\n")
    init_docker().start(path_to_log, slash)
    logger.info("End work build function.\n")
    
if __name__ == "__main__":
    build()