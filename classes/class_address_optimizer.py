from classes.class_db_connect import DBConnect
from utils.performance import performance
import os
from datetime import datetime
import pandas as pd


class AddressOptimizer():
    _header_map = {
        'address': [
            'str_num',
            'str_num_sfx',
            'str_pfx',
            'str',
            'str_sfx',
            'str_sfx_dir',
            'str_unit'
        ],
        'city': 'site_city',
        'state': 'state',
        'zip code': 'site_zip',
        'zone': ''
    }
    _states_data_set = {
        'Alabama': 'AL',
        'Alaska': 'AK',
        'Arizona': 'AZ',
        'Arkansas': 'AR',
        'California': 'CA',
        'Colorado': 'CO',
        'Connecticut': 'CT',
        'Delaware': 'DE',
        'Florida': 'FL',
        'Georgia': 'GA',
        'Hawaii': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL',
        'Indiana': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS',
        'Kentucky': 'KY',
        'Louisiana': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD',
        'Massachusetts': 'MA',
        'Michigan': 'MI',
        'Minnesota': 'MN',
        'Mississippi': 'MS',
        'Missouri': 'MO',
        'Montana': 'MT',
        'Nebraska': 'NE',
        'Nevada': 'NV',
        'New Hampshire': 'NH',
        'New Jersey': 'NJ',
        'New Mexico': 'NM',
        'New York': 'NY',
        'North Carolina': 'NC',
        'North Dakota': 'ND',
        'Ohio': 'OH',
        'Oklahoma': 'OK',
        'Oregon': 'OR',
        'Pennsylvania': 'PA',
        'Rhode Island': 'RI',
        'South Carolina': 'SC',
        'South Dakota': 'SD',
        'Tennessee': 'TN',
        'Texas': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT',
        'Virginia': 'VA',
        'Washington': 'WA',
        'West Virginia': 'WV',
        'Wisconsin': 'WI',
        'Wyoming': 'WY',
    }
    _file_path = './data/test.csv'
    _delimiter = ','
    _export_type = 'csv'
    _dir = './export'
    _filename = 'address-export-'
    _file = ''
    _datetime = datetime.now()
    _datetime_format = '%Y-%m-%d-%H%M%S'
    _per_page = 5000
    _addresses = set()
    _cities = set()
    _states = set()
    _zip_codes = set()
    _zones = set()
    _data = dict()
    _indxs = dict()
    _full_addresses = set()
    _db_data = dict()
    _dupes = dict()
    _fresh = dict()

    def __init__(self):
        pass

    def get_file_path(self):
        return self._file_path

    def get_header_map(self):
        return self._header_map

    def get_delimiter(self):
        return self._delimiter

    def get_export_type(self):
        return self._export_type

    def get_per_page(self):
        return self._per_page

    def set_dir(self, dir):
        self._dir = dir

    def get_dir(self):
        return self._dir

    def set_filename(self, filename):
        self._filename = filename

    def get_filename(self):
        return self._filename

    def set_file(self, file):
        self._file = file

    def get_file(self):
        return self._file

    def set_datetime(self):
        self._datetime = datetime.now()

    def get_datetime(self):
        return self._datetime

    def set_datetime_format(self, datetime_format):
        self._datetime_format = datetime_format

    def get_datetime_format(self):
        return self._datetime_format

    def set_file_path(self, file_path):
        self._file_path = file_path

    def set_header_map(self, header_map):
        self._header_map = header_map

    def set_delimiter(self, delimiter):
        self._delimiter = delimiter

    def set_export_type(self, export_type):
        self._export_type = export_type

    def set_per_page(self, per_page):
        self._per_page = per_page

    def set_address_indxs(self, headers):
        address_indxs = set()
        for address_field in self._header_map['address']:
            address_indxs.add(headers.index(address_field))
        return address_indxs

    def get_address_indxs(self):
        if 'address' in self._indxs.keys():
            return self._indxs['address']
        else:
            return

    def get_addresses(self):
        return self._addresses

    def get_city_indx(self):
        if 'city' in self._indxs.keys():
            return self._indxs['city']
        else:
            return

    def get_cities(self):
        return self._cities

    def get_state_indx(self):
        if 'state' in self._indxs.keys():
            return self._indxs['state']
        else:
            return

    def get_states(self):
        return self._states

    def get_zip_code_indx(self):
        if 'zip_code' in self._indxs.keys():
            return self._indxs['zip_code']
        else:
            return

    def get_zip_codes(self):
        return self._zip_codes

    def get_data(self):
        return self._data

    def get_db_data(self):
        return self._db_data

    def get_zone_indx(self):
        if 'zone' in self._indxs.keys():
            return self._indxs['zone']
        else:
            return

    def get_zones(self):
        return self._zones

    def get_full_address(self):
        return self._full_addresses

    def process_address(self, line, indxs=-1):
        indxs = self.get_address_indxs() if indxs == -1 else [indxs]
        address = ''

        for indx in indxs:
            address += line[indx] + ' '

        address = address.strip()
        address = address.replace("# ", "#")
        address_split = address.split()
        address = ' '.join(address_split)
        return address

    def process_city(self, line, indxs=-1):
        indx = self.get_city_indx() if indxs == -1 else indxs

        if (indx):
            return line[indx]
        else:
            return

    def process_state(self, line, indxs=-1):
        indx = self.get_state_indx() if indxs == -1 else indxs
        state_name = line[indx]
        state_name = self._states_data_set.get(state_name.title(), ' ').upper() if len(
            state_name) >= 3 else state_name.upper()

        return state_name

    def process_zip_code(self, line, indxs=-1):
        indx = self.get_zip_code_indx() if indxs == -1 else indxs

        if (indx):
            return line[indx]
        else:
            return

    def process_zone(self, line, indxs=-1):
        indx = self.get_zone_indx() if indxs == -1 else indxs

        if (indx):
            return line[indx]
        else:
            return ''

    def set_indxs(self, headers, mapping):
        key = ''
        for field in mapping:
            if field != 'address' and len(mapping[field]) > 0:
                key = mapping[field] if len(mapping[field]) > 0 else None
            else:
                if (type(mapping[field]) is list):
                    self._indxs[field] = self.set_address_indxs(headers)
                else:
                    key = mapping[field]
            try:
                field_split = field.split()
                field = '_'.join(field_split)
                self._indxs[field] = headers.index(key)
            except ValueError:
                pass

    def pandas_load(self):
        df = pd.read_csv(self._file_path,
                         dtype={"str_num": "string", "str_num_sfx": "string", "str_pfx": "string", "str": "string",
                                "str_sfx": "string", "str_sfx_dir": "string", "str_unit": "string",
                                "site_city": "string", "site_zip": "string", "state": "string", "county": "string"})
        address_str = ''
        # df[df['site_zip'].isnull()]
        print(df[df['str_num_sfx'].notnull()])

        df['address'] = address_str
        return df

    @performance
    def load_file(self):
        with open(self._file_path, mode='r') as file:
            headers = next(file).rstrip('\n')
            headers_list = list(map(str.strip, headers.split(self._delimiter)))

            self.set_indxs(headers_list, self._header_map)

            for line in file:
                line_list = list(map(str.strip, line.split(self._delimiter)))
                addy = ''

                if (self.process_address(line_list) != ''):
                    self._addresses.add(self.process_address(line_list))
                    addy += self.process_address(line_list) + ','

                if (self.process_city(line_list) != ''):
                    self._cities.add(self.process_city(line_list))
                    addy += self.process_city(line_list) + ','

                if (self.process_state(line_list) != ''):
                    self._states.add(self.process_state(line_list))
                    addy += self.process_state(line_list) + ','

                if (self.process_zip_code(line_list) != ''):
                    self._zip_codes.add(self.process_zip_code(line_list))
                    addy += self.process_zip_code(line_list) + ','

                if (self.process_zone(line_list) != ''):
                    self._zones.add(self.process_zone(line_list))

                self._full_addresses.add(addy)

                self._data[self.process_address(line_list)] = {
                    'address': self.process_address(line_list),
                    'city': self.process_city(line_list),
                    'state': self.process_state(line_list),
                    'zip_code': self.process_zip_code(line_list),
                    'zone': self.process_zone(line_list)
                }

    @performance
    def export_file(self):
        self.set_datetime()
        dir = self.get_dir()
        format = self.get_datetime_format()
        timestamp = self.get_datetime().strftime(format)
        file_type = self._export_type
        filename = self.get_filename() + timestamp
        nfile = filename + '.' + file_type
        rel_file_path = dir + '/' + nfile
        delimeter = self._delimiter
        data = self.get_data()

        self.set_file(nfile)

        if not os.path.exists(dir):
            os.makedirs(dir)

        with open(rel_file_path, mode='x') as f:
            f.write(
                'address' + delimeter + 'city' + delimeter + 'state' + delimeter + 'zip code' + delimeter + 'zone' + delimeter + '\n')
            for key, value in data.items():
                f.write(value['address'] + delimeter + value['city'] + delimeter + value['state'] + delimeter + value[
                    'zip_code'] + delimeter + value[
                            'zone'] + delimeter + '\n')

    @staticmethod
    def test_db():
        dbconnect = DBConnect()
        host = dbconnect.get_host()
        database = dbconnect.get_database()
        dbuser = dbconnect.get_dbuser()
        dbpwd = dbconnect.get_dbpassword()
        port = dbconnect.get_port()
        db = DBConnect.connect(host, database, dbuser, dbpwd, port)
        db.close()

    def connect_db(self):
        dbconnect = DBConnect()
        host = dbconnect.get_host()
        database = dbconnect.get_database()
        dbuser = dbconnect.get_dbuser()
        dbpwd = dbconnect.get_dbpassword()
        port = dbconnect.get_port()
        return DBConnect.connect(host, database, dbuser, dbpwd, port)

    @performance
    def pull_db(self):
        # addresses = self.get_data()
        conn = self.connect_db()
        cursor = conn.cursor(buffered=True)
        # query = ("SELECT address, city, state, zip_code FROM muniregdb.property")
        query = ("""
        SELECT muniregdb.property.address, muniregdb.property.city, muniregdb.property.state, muniregdb.property.zip_code, muniregdb.zone.name
        FROM muniregdb.property
        INNER JOIN muniregdb.property_zone
        ON muniregdb.property.id = muniregdb.property_zone.property_id
        INNER JOIN muniregdb.zone
        ON muniregdb.property_zone.zone_id = muniregdb.zone.id""")

        cursor.execute(query)

        for line in cursor:
            self._db_data[self.process_address(line, 0)] = {
                'address': self.process_address(line, 0),
                'city': self.process_city(line, 1),
                'state': self.process_state(line, 2),
                'zip_code': self.process_zip_code(line, 3),
                # 'zone': self.process_zone(line_list)
            }

        cursor.close()
        conn.close()

    @performance
    def compare_data(self):
        new_data = self.get_data()
        old_data = self.get_db_data()

        count = 0

        needles = set(new_data.keys())
        haystack = old_data

        # print(len(needles))
        # print(len(haystack))
        # print(len(new_data))
        # print(len(old_data))

        for needle in needles:
            if needle in haystack:
                count += 1
                self._dupes[needle] = needle
            else:
                self._fresh[needle] = needle

        #print(count)
        #print(len(self._dupes))
        #print(len(self._fresh))
