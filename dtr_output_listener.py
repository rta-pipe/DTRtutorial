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
from sys import argv
import redis
import configparser

from ast import literal_eval

if __name__ == '__main__':

    if len(argv) < 2:
        print("Please enter: \n - the path to the configuration file \n - The input channel to listen")
        exit()


    configFilePath = argv[1]
    inputChannel = argv[2]

    config = configparser.ConfigParser()
    config.read(configFilePath)


    print("Loading plot..")



    redisConn = redis.Redis(
                                host     = config.get('Redis','host'),
                                port     = config.getint('Redis','port'),
                                db       = config.getint('Redis','dbname'),
                                password = config.get('Redis','password')
                            )

    pubsub = redisConn.pubsub()
    pubsub.subscribe(inputChannel)


    print('[DTR] Subscribed to channel {} , waiting for messages...'.format(inputChannel))
    for message in pubsub.listen():
        if message['type'] != 'subscribe' and message['type'] != 'unsubscribe':

            print('\n\n New message arrived!!! ({})'.format(message['type']))

            decodedData = message['data'].decode("utf-8")
            dictData = literal_eval(decodedData)
            #histoData = dictData['last_data']['histogramValues']
            print("Message payload: ", dictData)
