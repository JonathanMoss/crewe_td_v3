#!/usr/bin/python3
import pika
import json
import threading
from os import path
import csv
import re
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from queue import Queue
from ast import literal_eval

CONFIG_FILE = 'conf/config.json'
SCALE = 16
NUM_OF_BITS = 8
td_matrix = {}
routing_table = {}
SVG_CLIENT = None
SVG_FILE = None
update_lock = threading.Lock()
thread_queue_lock = threading.Lock()
thread_queue = Queue()
berth_list = []
JSON_SOP = None
TD_AREAS = None
svg_handler = None


class SvgClient(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self._broker_ip = None
        self._broker_port = None
        self._incoming_broker_queue = None
        self._outgoing_broker_queue = None
        self._incoming_broker_user_name = None
        self._incoming_broker_password = None
        self._channel = None
        self._outgoing_credentials = None
        self._incoming_parameters = None
        self._connection = None
        self._svg = None
        self._csv = None
        self._sop_json = None
        self._td_areas = None
        self._get_configuration()
        self._create_incoming_broker_connection()

    def run(self):
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:

            print('Attempting to reconnect to message broker(pika)...')
            print(e)

        except Exception as e:

            print('Attempting to reconnect to message broker(other)...')
            print(e)

    def _message_callback(self, ch, method, properties, body):

        ch.basic_ack(delivery_tag=method.delivery_tag)
        body = literal_eval(body.decode('utf-8'))
        IncomingMessageHandler.incoming_msg(body)

    def _get_configuration(self):
        with open(CONFIG_FILE) as js_file:
            data = json.load(js_file)
            self._broker_ip = data['BROKER']['IP']
            self._broker_port = data['BROKER']['PORT']
            self._incoming_broker_queue = data['BROKER']['FILTERED_TD_FEED']['QUEUE_NAME']
            self._incoming_broker_user_name = data['BROKER']['FILTERED_TD_FEED']['CONSUMER']['USER_NAME']
            self._incoming_broker_password = data['BROKER']['FILTERED_TD_FEED']['CONSUMER']['PASSWORD']
            self._outgoing_broker_queue = data['BROKER']['FTP_FEED']['QUEUE_NAME']
            self._outgoing_broker_user_name = data['BROKER']['FTP_FEED']['PRODUCER']['USER_NAME']
            self._outgoing_broker_password = data['BROKER']['FTP_FEED']['PRODUCER']['PASSWORD']
            self._outgoing_credentials = pika.PlainCredentials(username=self._outgoing_broker_user_name,
                                                               password=self._outgoing_broker_password)
            self._incoming_credentials = pika.PlainCredentials(username=self._incoming_broker_user_name,
                                                               password=self._incoming_broker_password)
            self._incoming_parameters = pika.ConnectionParameters(host=self._broker_ip,
                                                                  port=self._broker_port,
                                                                  virtual_host='/',
                                                                  heartbeat_interval=90,
                                                                  retry_delay=5,
                                                                  socket_timeout=90,
                                                                  connection_attempts=10,
                                                                  credentials=self._incoming_credentials)
            self._outgoing_parameters = pika.ConnectionParameters(host=self._broker_ip,
                                                                  port=int(self._broker_port),
                                                                  virtual_host='/',
                                                                  credentials=self._outgoing_credentials)
            self._send_message_properties = pika.BasicProperties(expiration='100000', )

            self._svg = data['FILES']['SVG']
            self._csv = data['FILES']['CSV']
            global SVG_FILE
            SVG_FILE = self._svg
            self._sop_json = data['FILES']['SOP']
            global JSON_SOP
            JSON_SOP = self._sop_json
            global TD_AREAS
            self._td_areas = str(data['TD_AREAS']['MAPS']).split(',')
            TD_AREAS = self._td_areas
            print(TD_AREAS)

    def _create_incoming_broker_connection(self):
        self._connection = pika.BlockingConnection(self._incoming_parameters)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._incoming_broker_queue)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(self._message_callback, queue=self._incoming_broker_queue)

    def outgoing_broker_connection(self, msg):
        connection = pika.BlockingConnection(self._outgoing_parameters)
        channel = connection.channel()
        channel.queue_declare(queue=self._outgoing_broker_queue)
        channel.basic_publish(body=msg, exchange='', routing_key=self._outgoing_broker_queue,
                              properties=self._send_message_properties)
        connection.close()

    def get_csv(self):
        return self._csv


class SOPBuilder:

    @staticmethod
    def fill_matrix(client):

        """ This method reads in the details from the CSV and populates the td_matrix array """

        total_trts = 0
        total_signal = 0
        total_route = 0

        sop_csv = client.get_csv()

        if path.isfile(sop_csv):
            with open(sop_csv) as csv_file:
                csv_data = csv.reader(csv_file, delimiter=',')

                for row in csv_data:

                    address = str(row[1]).zfill(2)
                    bit = str(row[2])
                    detail = str(row[3])
                    context = str(row[4])
                    value = 0

                    if detail.startswith('P'):  # TRTS
                        total_trts += 1
                    elif detail.startswith('S'):  # Signal State
                        total_signal += 1
                        if 'GL' not in detail:
                            detail = re.findall(r'[0-9]{3,4}', detail)
                            detail = 'CE{}'.format(detail[0])
                        else:
                            detail = re.findall(r'[0-9]{3,4}', detail)
                            detail = 'GL{}'.format(detail[0])
                    elif detail.startswith('R'):  # Route
                        context = str(row[5])
                        total_route += 1
                        if row[12]:
                            tracks = str(row[12]).replace(' ', '')
                            routing_tracks = tracks.split(',')
                            routing_table.update({row[3]: routing_tracks})

                    if address not in td_matrix:
                        td_matrix.update({address: [{'bit': bit, 'detail': detail, 'context': context, 'value': value}]})

                    else:
                        if bit not in td_matrix[address]:

                            td_matrix[address].append({'bit': bit, 'detail': detail, 'context': context, 'value': value})

            print('Summary of {}...'.format(sop_csv))
            print('\t...{} signals found'.format(total_signal))
            print('\t...{} routes found'.format(total_route))
            print('\t...{} TRTS found'.format(total_trts))
        else:
            print('Cannot find csv')

    @staticmethod
    def print_json_to_file():

        """ This method prints the contents of the td_matrix to a json file """

        global JSON_SOP
        file_name = JSON_SOP

        try:
            with open(file_name, 'w') as jf:
                json.dump(td_matrix, jf, indent=2)
        except Exception as e:

            print('....unable to write json file.')
            return

        finally:
            print('....{} created.'.format(file_name))


class SVGHandler:

    svg_file = None
    tree = None
    root = None

    thread_number = 0

    def __init__(self, svg):

        self.svg_file = svg
        self.tree = ET.ElementTree(file=self.svg_file)
        self.root = self.tree.getroot()

    def clear_all_routes(self):

        with update_lock:
            for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g[@id="base_layer"]'):
                for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}g[@id="crewe_basic_track_layout"]'):
                    for e in sub_elem.iterfind('.//{http://www.w3.org/2000/svg}path'):
                        style = e.get('style')
                        style = re.sub(r'fill:#[0-9A-Za-z]*', 'fill:#ffffff', style)
                        e.set('style', style)
                        # e.set('onmouseover', 'this.style.stroke = "#ff0000"; this.style["stroke-width"] = 5;')
                        # e.set('onmouseout', 'this.style.stroke = "#000000"; this.style["stroke-width"] = 1;')
                        id = str(e.attrib['id'])
                        result = re.sub(r'[^0-9]+', '', id)
                        for x in routing_table:
                            for trk in routing_table[x]:
                                if trk == result:
                                    e.set('onmouseover',
                                          "var fill = this.style.fill; this.style.fill = 'red'; "
                                          "this.onmouseout = function() {this.style.fill = fill;}")
                                    e.set('onclick',
                                          "var track_id = this.id;"
                                          "var stripped_id = track_id.replace( /^\D+/g, '');"
                                          "var stroke_width = this.style.strokeWidth;"
                                          "if (stroke_width > 0) {"
                                          "this.style.strokeWidth = '0';"
                                          "var ind = global_tracks.indexOf(stripped_id);"
                                          "if (ind != -1){"
                                          "global_tracks.splice(ind, 1);}"
                                          "} else {"
                                          "this.style.stroke = 'black';"
                                          "this.style.strokeWidth = '0.559';"
                                          "if (global_tracks.indexOf(stripped_id) == -1){"
                                          "global_tracks.push(stripped_id);}"
                                          "console.log(global_tracks);"
                                          "}")
                                    break



    def set_route(self, route, set_route=True):

        with update_lock:
            if route in routing_table:
                for trk in routing_table[route]:
                    search_string = './/{{http://www.w3.org/2000/svg}}path[@id="track_{}"]'.format(trk)
                    for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g[@id="base_layer"]'):
                        for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}g[@id="crewe_basic_track_layout"]'):
                            for e in sub_elem.iterfind(search_string):
                                style = e.get('style')
                                if set_route:
                                    style = re.sub(r'fill:#[0-9A-Za-z]*', 'fill:#08db1d', style)
                                else:
                                    style = re.sub(r'fill:#[0-9A-Za-z]*', 'fill:#ffffff', style)
                                e.set('style', style)

    def delete_all_berth_text(self):

        with update_lock:
            for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g'):
                if 'Berth' in str(element.attrib):
                    berth_list.append(element.get('id'))
                    for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}tspan'):
                        sub_elem.text = ''

    def interpose_description(self, description, berth):

        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}g[@id="{}"]'.format(berth)
            for elem in self.root.iterfind(search_string):
                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}tspan'):
                    sub_elem.text = description

    def clear_berth(self, berth):

        self.interpose_description('', berth)

    def clear_all_trts(self):

        with update_lock:
            search_string = './/{http://www.w3.org/2000/svg}g[@id="trts"]'
            for elem in self.root.iterfind(search_string):

                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}circle'):
                    style = sub_elem.get('style')
                    style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffe355', style)
                    sub_elem.set('style', style)

    def set_signal(self, signal, signal_on=True):

        with update_lock:
            search_string = ".//{{http://www.w3.org/2000/svg}}g[@id='{}']".format(signal)

            position_light = False

            for elem in self.root.iterfind(search_string):

                if 'Ground Position Light' in elem.attrib['{http://www.inkscape.org/namespaces/inkscape}label']:

                    position_light = True

                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}ellipse'):

                    style_attrib = sub_elem.get('style')

                    if signal_on:
                        style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#ff0000', style_attrib)

                    else:
                        style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#59f442', style_attrib)

                    sub_elem.set('style', style_attrib)

                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}circle'):

                    style_attrib = sub_elem.get('style')

                    if signal_on:
                        style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#ff0000', style_attrib)
                    else:
                        if position_light:
                            style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffffff', style_attrib)
                        else:
                            style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#59f442', style_attrib)

                    sub_elem.set('style', style_attrib)

    def clear_trts(self, trts):

        """ This method indicates that a TRTS has been cancelled """

        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}circle[@id="{}"]'.format(trts)
            for elem in self.root.iterfind(search_string):
                style = elem.get('style')
                style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffe355', style)
                elem.set('style', style)

    def show_trts(self, trts):

        """ This method indicates that a TRTS button has been pressed """

        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}circle[@id="{}"]'.format(trts)
            for elem in self.root.iterfind(search_string):
                style = elem.get('style')
                style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffffff', style)
                elem.set('style', style)

    def queue_thread(self):

        """ This method is ran as a thread, and starts threads in the thread queue, max of 10 at a time"""

        while True:
            if not thread_queue.empty():
                with thread_queue_lock:
                    if thread_queue.qsize() < 10:
                        mx = thread_queue.qsize()
                    else:
                        mx = 10

                    for x in range(mx):
                        m = thread_queue.get()
                        m.setName(self.thread_number)
                        m.start()
                        m.join()
                        thread_queue.task_done()
                        self.thread_number += 1

                with update_lock:
                    self.tree.write('svg/working.svg')
                    global SVG_CLIENT
                    tree_string = ET.tostring(self.root, encoding='utf-8', method='xml')
                    SVG_CLIENT.outgoing_broker_connection(tree_string)


class IncomingMessageHandler:

    @staticmethod
    def make_valid_headcode(description):

        letter = re.findall('[A-Z]', description)
        if len(letter) > 1:
            return description
        else:
            numbers = re.findall('[0-9]', description)
            if len(numbers) == 3:
                new_headcode = '{}{}{}{}'.format(numbers[0], letter[0], numbers[1], numbers[2])
                return new_headcode
            else:
                return description

    @staticmethod
    def incoming_msg(incoming_message):
        global TD_AREAS
        global berth_list
        global svg_handler
        for k, v in incoming_message.items():

            with thread_queue_lock:
                if k == 'CA_MSG':  # Berth Step Message
                    description = v['descr']
                    if not re.match(r'^[0-9][A-Z][0-9][0-9]$', description):
                        description = IncomingMessageHandler.make_valid_headcode(description)

                    berth_from = '{}_{}'.format(v['area_id'], v['from'])
                    berth_to = '{}_{}'.format(v['area_id'], v['to'])
                    print('Berth Step Message for {} from {} to {}'.format(description, berth_from, berth_to))

                    x = threading.Thread(target=svg_handler.interpose_description, args=(description, berth_to))
                    x.daemon = True
                    thread_queue.put(x)
                    y = threading.Thread(target=svg_handler.clear_berth, args=(berth_from, ))
                    y.daemon = True
                    thread_queue.put(y)

                elif k == 'CC_MSG':  # Interpose Message
                    description = v['descr']
                    if not re.match(r'^[0-9][A-Z][0-9][0-9]$', description):
                        description = IncomingMessageHandler.make_valid_headcode(description)

                    berth = '{}_{}'.format(v['area_id'], v['to'])
                    x = threading.Thread(target=svg_handler.interpose_description, args=(description, berth))
                    x.daemon = True
                    thread_queue.put(x)
                    print('Berth Interpose Message for {} to {}'.format(description, berth))

                elif k == 'CB_MSG':  # Cancel Message
                    berth = '{}_{}'.format(v['area_id'], v['from'])
                    y = threading.Thread(target=svg_handler.clear_berth, args=(berth,))
                    y.daemon = True
                    thread_queue.put(y)
                    print('Berth Cancel Message from {}'.format(berth))

                elif k == 'SF_MSG' and v['area_id'] == TD_AREAS[0]:  # S Class Message
                    x = threading.Thread(target=IncomingMessageHandler.signalling_update, args=(v, ))
                    x.daemon = True
                    thread_queue.put(x)

                elif (k == 'SG_MSG' or k == 'SH_MSG') and v['area_id'] == TD_AREAS[0]:
                    start_address = v['address']
                    data = re.findall('..', v['data'])
                    for d in data:

                        x = threading.Thread(
                            target=IncomingMessageHandler.signalling_update,
                            args=({'address': start_address, 'data': d}, ))
                        x.daemon = True
                        thread_queue.put(x)
                        start_address = int(start_address, 16)
                        start_address += 1
                        start_address = hex(start_address)[2:].zfill(2).upper()

    @staticmethod
    def signalling_update(v):

        address = v['address']  # Get the address from the message
        h_data = v['data']  # Get the message data

        hex_data = bin(int(h_data, SCALE))[2:].zfill(NUM_OF_BITS)  # Convert the message data to 8 bit binary string
        rev_hex_data = hex_data[::-1]  # Reverse the binary string (LSB first)

        for i in range(8):
            bit = int(rev_hex_data[i])  # Get the Nth bit
            if address in td_matrix:  # Check if the address is found within sig_matrix
                for x in range(len(td_matrix[address])):  # Loop though the length of all bits within the address
                    if int(td_matrix[address][x]['bit']) == i:  # Find the correct bit
                        curr_value = td_matrix[address][x]['value']  # Assign the current bit value to a variable
                        if curr_value != bit:  # The received bit value is different the that currently stored
                            td_matrix[address][x]['value'] = bit  # Update the current bit value
                            if str(td_matrix[address][x]['detail']).startswith('R'):

                                if td_matrix[address][x]['value'] == 1:
                                    svg_handler.set_route(td_matrix[address][x]['detail'])
                                else:
                                    svg_handler.set_route(td_matrix[address][x]['detail'], False)

                            if td_matrix[address][x]['context'] == 'Signal State':
                                if td_matrix[address][x]['value'] == 1:
                                    svg_handler.set_signal(td_matrix[address][x]['detail'], False)
                                else:

                                    svg_handler.set_signal(td_matrix[address][x]['detail'], True)

                            if td_matrix[address][x]['context'] == 'TRTS':

                                if td_matrix[address][x]['value'] == 1:

                                    svg_handler.show_trts(td_matrix[address][x]['detail'])
                                else:

                                    svg_handler.clear_trts(td_matrix[address][x]['detail'])

                        break
            else:  # Unknown Address, not found in sig_matrix
                pass


def main():

    global SVG_CLIENT
    global svg_handler
    SVG_CLIENT = SvgClient()
    SOPBuilder.fill_matrix(SVG_CLIENT)
    SOPBuilder.print_json_to_file()
    svg_handler = SVGHandler(SVG_FILE)
    svg_handler.delete_all_berth_text()
    svg_handler.clear_all_trts()
    svg_handler.clear_all_routes()
    th = threading.Thread(target=svg_handler.queue_thread)
    th.setName('Queue Manager Thread')
    th.daemon = True
    th.start()
    SVG_CLIENT.start()

if __name__ == '__main__':
    main()



