#!/usr/bin/python3
from svg_berth import process_config
from mq import Mq
CONFIG_FILE = 'conf/config.json'


def main():

    process_config()
    td_mq = Mq()
    td_mq.connect()


if __name__ == '__main__':
    main()