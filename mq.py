import stomp
import time
import json
import logging
import pika

LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'

logging.basicConfig(filename='crewe_td.log',
                    level=logging.INFO,
                    format=LOG_FORMAT,
                    filemode='w')

logger = logging.getLogger()
BERTH_LIST = None
TD_AREA_LIST = None
S_CLASS = None
BROKER = None


class Broker:

    def __init__(self):
        self._CONFIG_FILE = 'conf/config.json'
        self._broker_ip = ''
        self._broker_port = ''
        self._broker_queue = ''
        self._broker_user_name = ''
        self._broker_password = ''
        self._get_credentials()
        self._mb_credentials = pika.PlainCredentials(username=self._broker_user_name,
                                                     password=self._broker_password)
        self._mb_parameters = pika.ConnectionParameters(host=self._broker_ip,
                                                        port=int(self._broker_port),
                                                        virtual_host='/',
                                                        credentials=self._mb_credentials)
        self._send_message_properties = pika.BasicProperties(expiration='100000', )

    def _get_credentials(self):
        with open(self._CONFIG_FILE) as js_file:
            data = json.load(js_file)
            self._broker_ip = data['BROKER']['IP']
            self._broker_port = data['BROKER']['PORT']
            self._broker_queue = data['BROKER']['FILTERED_TD_FEED']['QUEUE_NAME']
            self._broker_user_name = data['BROKER']['FILTERED_TD_FEED']['PRODUCER']['USER_NAME']
            self._broker_password = data['BROKER']['FILTERED_TD_FEED']['PRODUCER']['PASSWORD']

    def send_to_broker(self, msg):
        print(msg)
        connection = pika.BlockingConnection(self._mb_parameters)
        channel = connection.channel()
        channel.queue_declare(queue=self._broker_queue)
        channel.basic_publish(body=msg, exchange='', routing_key=self._broker_queue, properties=self._send_message_properties)
        connection.close()


class MqListener(stomp.ConnectionListener):

    def __init__(self, conn):
        self.conn = conn

    def on_error(self, headers, message):
        print(message)

    @staticmethod
    def is_berth_valid(td_area, td_berth):
        check_for = '{}_{}'.format(td_area, td_berth)
        if check_for in BERTH_LIST:
            return True
        return False

    def on_message(self, headers, message):
        global BROKER
        json_msg = json.loads(message)
        for msg in json_msg:
            for k, v in msg.items():
                if v['area_id'] in TD_AREA_LIST:
                    if v['msg_type'] == 'CA':  # Berth Step
                        if self.is_berth_valid(v['area_id'], v['from']) or self.is_berth_valid(v['area_id'], v['to']):
                            BROKER.send_to_broker(json.dumps(msg))
                    elif v['msg_type'] == 'CC':  # Interpose
                        if self.is_berth_valid(v['area_id'], v['to']):
                            BROKER.send_to_broker(json.dumps(msg))
                    elif v['msg_type'] == 'CB':  # Cancel
                        if self.is_berth_valid(v['area_id'], v['from']):
                            BROKER.send_to_broker(json.dumps(msg))
                    elif v['msg_type'] == 'CT':  # Heartbeat
                        pass
                    elif str(v['msg_type']).startswith('S'):  # S_Class Message
                        if v['area_id'] in S_CLASS:
                            BROKER.send_to_broker(json.dumps(msg))

    def on_disconnected(self):
        Mq.connect_and_subscribe()


class Mq:

    def __init__(self):

        global BROKER
        BROKER = Broker()

        self._conn = None
        self._USER_N = ''
        self._PASS_W = ''
        self._TOPIC = ''
        self._VHOST = ''
        self._PORT = ''
        self._CONFIG_FILE = 'conf/config.json'
        self.get_credentials()

    def connect_and_subscribe(self):

        self._conn.start()
        self._conn.connect(self._USER_N, self._PASS_W, wait=True)
        self._conn.subscribe(destination=self._TOPIC, ack='auto', id=self._TOPIC)

    def _get_connection(self):

        self._conn = stomp.Connection(host_and_ports=[(self._VHOST, self._PORT)],
                                      keepalive=False,
                                      vhost=self._VHOST)
        self._conn.set_listener('', MqListener(self._conn))

    def get_credentials(self):

        global TD_AREA_LIST, BERTH_LIST, S_CLASS

        with open(self._CONFIG_FILE) as js_file:
            data = json.load(js_file)
            self._USER_N = data['NROD_CREDENTIALS']['USER_N']
            self._PASS_W = data['NROD_CREDENTIALS']['PASS_W']
            self._VHOST = data['NROD_CREDENTIALS']['VHOST']
            self._PORT = data['NROD_CREDENTIALS']['PORT']
            self._TOPIC = '/topic/{}'.format(data['TD_AREAS']['TOPIC'])
            TD_AREA_LIST = data['TD_AREAS']['MAPS']
            BERTH_LIST = data['TD_AREAS']['BERTHS']
            S_CLASS = data['TD_AREAS']['S_CLASS']

    def connect(self):

        if self._conn is None:
            self._get_connection()

        self.connect_and_subscribe()

        while self._conn.is_connected():
            time.sleep(1)

        print('Disconnected')
