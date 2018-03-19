#!/usr/bin/python3
import pika
import json
from ftplib import FTP
import ftplib
import io
import logging
import datetime

CONFIG_FILE = 'conf/config.json'
WORKING_SVG = 'crewe_td_wrk.svg'
LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'

logging.basicConfig(filename='ftp.log',
                    level=logging.INFO,
                    format=LOG_FORMAT,
                    filemode='w')

logger = logging.getLogger()


class FtpClient:

    def __init__(self):
        self._broker_ip = None
        self._broker_port = None
        self._incoming_broker_queue = None
        self._incoming_broker_user_name = None
        self._incoming_broker_password = None
        self._channel = None
        self._credentials = None
        self._incoming_parameters = None
        self._connection = None
        self._ftp_user_name = None
        self._ftp_password = None
        self._ftp_host = None
        self._get_credentials()
        self._create_incoming_broker_connection()

    def _get_credentials(self):
        with open(CONFIG_FILE) as js_file:
            data = json.load(js_file)
            self._broker_ip = data['BROKER']['IP']
            self._broker_port = data['BROKER']['PORT']
            self._incoming_broker_queue = data['BROKER']['FTP_FEED']['QUEUE_NAME']
            self._incoming_broker_user_name = data['BROKER']['FTP_FEED']['CONSUMER']['USER_NAME']
            self._incoming_broker_password = data['BROKER']['FTP_FEED']['CONSUMER']['PASSWORD']
            self._ftp_user_name = data['FTP_CREDENTIALS']['USER_N']
            self._ftp_password = data['FTP_CREDENTIALS']['PASS_W']
            self._ftp_host = data['FTP_CREDENTIALS']['HOST']

            self._credentials = pika.PlainCredentials(username=self._incoming_broker_user_name,
                                                      password=self._incoming_broker_password)

            self._incoming_parameters = pika.ConnectionParameters(host=self._broker_ip,
                                                                  port=self._broker_port,
                                                                  virtual_host='/',
                                                                  heartbeat_interval=90,
                                                                  retry_delay=5,
                                                                  socket_timeout=90,
                                                                  connection_attempts=10,
                                                                  credentials=self._credentials)

    def _message_callback(self, ch, method, properties, body):

        try:
            now = datetime.datetime.now()
            if now.second % 2 == 0:
                with FTP(self._ftp_host, timeout=10) as ftp:
                    logger.info(ftp.login(user=self._ftp_user_name, passwd=self._ftp_password))
                    bio = io.BytesIO(body)
                    logger.info('{} ({} KB)'.format(ftp.storbinary('STOR ' + WORKING_SVG,
                                                                      bio,
                                                                      1024), int(ftp.size(WORKING_SVG) / 1024)))
        except ftplib.all_errors as e:
            logger.info(e)
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def _create_incoming_broker_connection(self):

        self._connection = pika.BlockingConnection(self._incoming_parameters)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._incoming_broker_queue)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(self._message_callback, queue=self._incoming_broker_queue)
        self._channel.start_consuming()


if __name__ == '__main__':
    ftp_client = FtpClient()
