#!/usr/bin/python3
import json
from pathlib import Path
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import re

CONFIG_FILE = 'conf/config.json'
SVG_FILE = None
td_map_list = []
berth_list = []


class ListBerthsFromFile:

    @staticmethod
    def check_files():

        """ This method checks that the Config and SVG files are present and correct """

        global SVG_FILE
        global CONFIG_FILE

        fl = Path(CONFIG_FILE)
        if fl.is_file():
            try:
                with open(CONFIG_FILE, 'r') as json_file:
                    data = json.load(json_file)
                    SVG_FILE = data['FILES']['SVG']
                    svg_fl = Path(SVG_FILE)
                    if svg_fl.is_file():
                        return True
                    else:
                        print("ERROR: Cannot locate the SVG file '{}' "
                              "as specified within '{}', cannot continue...".format(SVG_FILE, CONFIG_FILE))
            except KeyError as e:
                print('ERROR: Cannot find key/value pair that specifies the '
                      'location of the SVG file, cannot continue: {}'.format(e))
            except json.JSONDecodeError:
                print('ERROR: Malformed json file, cannot continue: {}'.format(CONFIG_FILE))
            except Exception:
                print('ERROR: Unspecified error - cannot continue.')

        else:
            print('Cannot locate the configuration file: {}'.format(CONFIG_FILE))

        return False

    @staticmethod
    def get_berths():

        """ This method gets the births and map references from the SVG file """

        global SVG_FILE
        global td_map_list
        global berth_list
        global CONFIG_FILE
        print("Getting Berth and TD Map data from '{}'...".format(SVG_FILE))

        tree = ET.ElementTree(file=SVG_FILE)
        root = tree.getroot()
        for element in root.iterfind('.//{http://www.w3.org/2000/svg}g'):
            if 'Berth' in str(element.attrib):
                berth_id = element.get('id')
                if re.match('^[a-zA-Z0-9]{2}_', berth_id):
                    td_map = str(berth_id[0:2])
                    if td_map not in td_map_list:
                        td_map_list.append(td_map)
                    if berth_id not in berth_list:
                        berth_list.append(berth_id)

        print("\t{} TD Map references and {} named TD Berths found".format(len(td_map_list), len(berth_list)))
        print("Updating {}...".format(CONFIG_FILE))
        ListBerthsFromFile._update_config_map_list(td_map_list)

    @staticmethod
    def _update_config_map_list(td_map_lst):

        """ This method updates the Config File with the berths and map references """

        global CONFIG_FILE
        try:
            with open(CONFIG_FILE, 'r') as json_file:
                data = json.load(json_file)

            data['TD_AREAS']['MAPS'] = ','.join(map(str, td_map_lst))
            data['TD_AREAS']['BERTHS'] = berth_list

            with open(CONFIG_FILE, 'w') as json_file:
                json.dump(data, json_file, indent=4)
        except Exception as err:
            print('\tFailed - {}'.format(err))
        else:
            print('\tSuccess! {} updated with TD map and berth level data'.format(CONFIG_FILE))


def process_config():

    print('\nValidating configuration file...')

    if ListBerthsFromFile.check_files():  # Check if the config file and svg are present and correct.
        print("\t'{}' and '{}' passed validation".format(CONFIG_FILE, SVG_FILE))
        ListBerthsFromFile.get_berths()   # Get the berts and TD map references from the SVG file.
    else:
        print('\tfailed, cannot continue')


if __name__ == '__main__':
    process_config()