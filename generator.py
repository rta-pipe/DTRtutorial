# ==========================================================================
#
# Copyright (C) 2018 INAF - OAS Bologna
# Author: Leonardo Baroncelli <leonardo.baroncelli@inaf.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# ==========================================================================

import sys
from time import sleep
from random import randint
import redis
from ast import literal_eval
import configparser
import csv

def sendMockData(config, redisConn, mockDetectionData, channel):

    for event in reversed(mockDetectionData):     # SORTING...
        sleep(randint(config.getint('General', 'sleepmin'),config.getint('General', 'sleepmax')))
        event['dataType'] = 'evt3'
        sendEvent(redisConn, event, channel)

    sendEvent(redisConn, None, channel, lastEvent = True)


def sendEvent(redisConn, event, channel, lastEvent = False):
    if not lastEvent:
        print("\nPublishing.. ",event," on DTR channel ",channel)
        redisConn.publish(channel, event)
    else:
        redisConn.publish(channel, 'STOP')


def getMockData(mockDataPath, fileFormat='csv'):
    print("Parsing data...")
    if fileFormat == 'json':
        mockdatafile = open(mockDataPath,'r')
        mockData = mockdatafile.read()
        return literal_eval(mockData);

    elif fileFormat == 'csv':
        data = []
        with open(mockDataPath, newline='') as csvfile:
            reader = list(csv.reader(csvfile, delimiter=','))
            keys = reader[0]
            print(keys)
            print("Data length: {}".format(len(reader)))
            for row in reader[2:]:
                dict = {}
                l = len(row)
                for i in range(l):
                    dict[keys[i]] = row[i]
                data.append(dict)

        print("Data length: {}".format(len(data)))
        return data

    else:
        print("Format not found.")


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Please enter: \n - the path to the configuration file \n - mock file path \n - the generator output channel")
        exit()

    a = [1, 3, 5]
    b = a
    a[:] = [x + 2 for x in a]
    print(b)

    configFilePath = sys.argv[1]
    mockDataPath = sys.argv[2]
    generatoroutputchannel = sys.argv[3]

    config = configparser.ConfigParser()
    config.read(configFilePath)

    redisConn = redis.Redis(
                                host     = config.get('Redis','host'),
                                port     = config.getint('Redis','port'),
                                db       = config.getint('Redis','dbname'),
                                password = config.get('Redis','password')
                            )

    mockData = getMockData(mockDataPath, 'csv')
    print("Example of data: ", mockData[0])

    sendMockData(config, redisConn, mockData, generatoroutputchannel )


    """
        Examples:

            CSV CTA:
                {'import_time': '1539884499.1339893',
                'rootname': '/data01/ANALYSIS3.local/CTA-SOUTH/DREVT3_CTA_O1/ctools-lc_LC1000-shift10/T442803540_442804540/T442803540_442804540_E0.1_100_P184.557449_-5.78436',
                'fluxul': 'NULL',
                'detectionid': '40636',
                'label': 'Crab-1',
                'l': '184.557',
                'b': '-5.7843',
                'ella': '-1',
                'ellb': '-1',
                'fluxerr': '0.317889041855604',
                'flux': '13.6674',
                'sqrtts': '110.566',
                'spectralindex': '2.43214',
                'spectralindexerr': '0.0203501',
                'tstart': '442803540',
                'tstop': '442804540',
                'emin': '0.1',
                'emax': '100',
                'run_l': '184.557449',
                'run_b': '-5.78436',
                'dataType': 'detection',
                'instrument_id': '1',
                'observation_id': '2',
                'analysis_session_type_id': '3'}


    """
