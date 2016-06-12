#!-*- encoding=utf-8
import requests
import httplib2
from operator import itemgetter
import random
import threading
from .exc import NoAvaliableVolumeError, PutFileException, GetFileException
TIMEOUT = 30

class VolumeLocationManager(object):
    def __init__(self, master_addr, master_port):
        self.master_addr = master_addr
        self.master_port = master_port
        self.session = requests.session()
        self.volume_cache = {}

    def _fetch_volume_location(self, volume_id):
        url = "http://{master_addr}:{master_port}/dir/lookup?volumeId={volume_id}".format(
            master_addr=self.master_addr,
            master_port=self.master_port,
            volume_id=volume_id)
        r = self.session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        locations = r.json()['locations']
        return list(map(itemgetter('publicUrl'), locations))

    def fetch_volume_location(self, volume_id):
        # save getter func to cache, raise Exception if volume not exist
        volume_id = int(volume_id)
        locations = self._fetch_volume_location(volume_id)
        if len(locations) == 0:
            raise NoAvaliableVolumeError("No avaliable Volume server for Volume %s" % volume_id)
        if len(locations) == 1:
            location = locations[0]
            self.volume_cache[volume_id] = lambda: location
        else:
            self.volume_cache[volume_id] = lambda: random.choice(locations)
        return

    def get_volume_location(self, volume_id):
        volume_id = int(volume_id)
        if volume_id not in self.volume_cache:
            self.fetch_volume_location(volume_id)
        return self.volume_cache[volume_id]()

class WeedFS(object):
    def __init__(self, master_addr, master_port=9333):
        self.master_addr = master_addr
        self.master_port = master_port
        self.volume_manager = VolumeLocationManager(self.master_addr, self.master_port)
        self._default_getter = None
        self.thread_data = threading.local()

    def __repr__(self):
        return "<{0} {1}:{2}>".format(
            self.__class__.__name__,
            self.master_addr,
            self.master_port)

    def get_requests_session(self):
        s = getattr(self.thread_data, 'rsession', None)
        if s is None:
            s = requests.session()
            self.thread_data.rsession = s
        return s

    def get_httplib2_session(self):
        s = getattr(self.thread_data, 'hsession', None)
        if s is None:
            s = httplib2.Http(timeout=30)
            self.thread_data.hsession = s
        return s

    def get(self, fid, image_thumbnail_size=None):
        volume_id, rest = fid.strip().split(",")
        volume_url = self.volume_manager.get_volume_location(volume_id)
        url = "http://{}/{}".format(volume_url, fid)
        if image_thumbnail_size is not None:
            assert isinstance(image_thumbnail_size, int) and image_thumbnail_size > 0
            url += '.jpg?height={0}&width={0}'.format(image_thumbnail_size)
        s = self.get_httplib2_session()
        resp, content = s.request(url)
        if resp.status != 200 and resp.status  != 201:
            raise GetFileException(resp.status)
        return content

    def put(self, fileobj, name, collection=None):
        s = self.get_requests_session()

        url = "http://{master_addr}:{master_port}/dir/assign".format(
            master_addr=self.master_addr,
            master_port=self.master_port)

        data = {}
        if collection is not None:
            assert isinstance(collection, str), 'collection must be a str'
            data['collection'] = collection

        res = s.get(url, params=data, timeout=30)
        res.raise_for_status()
        data = res.json()
        if data.get("error") is not None:
            raise PutFileException(data['error'])

        post_url = "http://{publicUrl}/{fid}".format(**data)

        res = s.post(post_url, files={name: fileobj}, timeout=30)
        res.raise_for_status()
        response_data = res.json()
        if 'size' in response_data:
            return data.get('fid')
        else:
            raise PutFileException('Unknown response: %s' % response_data)

    def drop_collection(self, collection):
        assert isinstance(collection, str), 'collection must be a str'
        assert len(collection) > 0
        s = self.get_requests_session()
        url = "http://{master_addr}:{master_port}/col/delete".format(
            master_addr=self.master_addr,
            master_port=self.master_port)

        res = s.get(url, params={'collection': collection})
        res.raise_for_status()
        return

    def grow_collection(self, collection, count):
        assert isinstance(collection, str), 'collection must be a str'
        assert len(collection) > 0
        assert isinstance(count, int) and count > 0
        s = self.get_requests_session()
        url = "http://{master_addr}:{master_port}/vol/grow".format(
            master_addr=self.master_addr,
            master_port=self.master_port)

        res = s.get(url, params={'collection': collection, 'count': count})
        res.raise_for_status()
        return
