import json
import sys

import asyncio
import socketio
from pymongo import MongoClient
from numpyencoder import NumpyEncoder

import signal

import loghandler

log_handler = loghandler.LogHandler()


class SocketFeed:

    def __init__(
        self,
        mongo_host,
        mongo_db,
        mongo_collection,
        socket_host,
        socket_token
    ):
        self.logger = log_handler.create_logger('socketfeed.SocketFeed')

        # MongoDB Client
        mongo_client = MongoClient(mongo_host)
        mongo_db = mongo_client[mongo_db]

        self.mongo_coll = mongo_db[mongo_collection]
        # &EIO=3
        self.socket_url = f"{socket_host}?joinServerParameters={socket_token}"

        self.logger.debug(f"self.socket_url: {self.socket_url}")

    def run(self):
        # Socket.io Client
        sio = socketio.AsyncClient()  # engineio_logger=self.logger)

        ## Socket.io Event Handlers ##

        @sio.on('connect')
        async def connect_handler():
            self.logger.info('Connected.')

        @sio.on('disconnect')
        async def disconnect_handler():
            self.logger.info('Disconnected.')

        @sio.on('history')
        async def history_handler(data):
            self.logger.debug(f'data: {data}')

        # @sio.on('send message')
        # async def send_message(data):
        #    self.logger.debug(f"send message: {data}")

        #    await sio.emit('send message', data['text'])

        @sio.on('chat message')
        async def message_handler(data):
            self.logger.debug(f"chat message: {data}")
            await message_consumer(data)

        ## Coroutines and Auxiliary Functions ##

        async def message_consumer(data):
            pass

        async def start_server(socket_url):
            await sio.connect(socket_url)
            self.logger.debug(f'sid: {sio.sid}')

        async def stop_server():
            await sio.disconnect()

        loop = asyncio.get_event_loop()

        loop.add_signal_handler(signal.SIGINT, stop_server)

        try:
            loop.run_until_complete(start_server(socket_url=self.socket_url))
            loop.run_forever()

        except KeyboardInterrupt:
            self.logger.info('Exit signal received.')

        # finally:
        #    loop.run_until_complete(stop_server())


if __name__ == '__main__':
    main_logger = log_handler.create_logger('socketmongo.main')

    MONGO_HOST = None
    MONGO_DB = None
    MONGO_COLLECTION = None
    SOCKET_HOST = None
    SOCKET_TOKEN = None

    import configparser

    config_path = 'settings.conf'

    config = configparser.RawConfigParser()
    config.read(config_path)

    if config['mongo']['host'] and config['mongo']['db'] and config['mongo']['collection']:
        main_logger.info('Found MongoDB settings in config file.')

        try:
            MONGO_HOST = config['mongo']['host']
            MONGO_DB = config['mongo']['db']
            MONGO_COLLECTION = config['mongo']['collection']
        except:
            main_logger.warning(
                'Failed to set MongoDB values from config file.')

    if config['socket']['host'] and config['socket']['token']:
        main_logger.info('Found Socket.io settings in config file.')

        try:
            SOCKET_HOST = config['socket']['host']
            SOCKET_TOKEN = config['socket']['token']

        except:
            main_logger.logger.error(
                'Could not load socket.io host and token from config file.')
            sys.exit(1)

    socket_feed = SocketFeed(
        mongo_host=MONGO_HOST,
        mongo_db=MONGO_DB,
        mongo_collection=MONGO_COLLECTION,
        socket_host=SOCKET_HOST,
        socket_token=SOCKET_TOKEN
    )

    socket_feed.run()
