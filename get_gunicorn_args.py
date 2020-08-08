import sys
from config import Config


if __name__ == '__main__':
    conf = Config()
    workers = conf.get_workers()
    bind = f'{conf.get_host()}:{conf.get_port()}'
    env_val = f'--bind={bind} --workers={workers}'
    sys.exit(env_val)
