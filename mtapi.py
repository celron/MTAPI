import urllib2, contextlib, datetime, copy
from collections import defaultdict
from operator import itemgetter
from pytz import timezone
import threading, time
import csv, math, json
import logging
import google.protobuf.message

from mtaproto.feedresponse import FeedResponse, Trip, TripStop

logger = logging.getLogger(__name__)

def distance(p1, p2):
    return math.sqrt((p2[0] - p1[0])**2 + (p2[1] - p1[1])**2)

class Mtapi(object):

    #300 seconds is 5 minutes, a little long
    #_LOCK_TIMEOUT = 300
    _LOCK_TIMEOUT = 120
    _tz = timezone('US/Eastern')
    _FEED_URLS = [
        # 1 2 3 4 5 6 S note that S becomes GS... does this make sense?
        'http://datamine.mta.info/mta_esi.php?feed_id=1',
        # L line
        'http://datamine.mta.info/mta_esi.php?feed_id=2',
        # this is SI
        'http://datamine.mta.info/mta_esi.php?feed_id=11',
        # this is the N Q R W still beta
        #'http://datamine.mta.info/mta_esi.php?feed_id=16',
        # this is the B D F M still beta
        'http://datamine.mta.info/mta_esi.php?feed_id=21'
    ]

    def __init__(self, key, stations_file, expires_seconds=None, max_trains=10, max_minutes=30, threaded=True):
        self._KEY = key
        self._MAX_TRAINS = max_trains
        self._MAX_MINUTES = max_minutes
        self._EXPIRES_SECONDS = expires_seconds
        self._THREADED = threaded
        self._stations = []
        self._stops = {}
        self._routes = {}
        #the thread that locked this must release it
        self._read_lock = threading.RLock()
        #any thread may release this lock
        self._update_lock = threading.Lock()

        self._init_feeds_key(key)

        # initialize the stations database
        try:
            with open(stations_file, 'rb') as f:
                self._stations = json.load(f)
                for idx, station in enumerate(self._stations):
                    station['id'] = idx

        except IOError as e:
            print 'Couldn\'t load stations file '+stations_file
            exit()

        self._update()

        if self._THREADED:
            self._start_timer()

    def _init_feeds_key(self, key):
        self._FEED_URLS = list(map(lambda x: x + '&key=' + key, self._FEED_URLS))

    def _start_timer(self):
        logger.info('Starting update thread...')
        self._timer_thread = threading.Thread(target=self._update_timer)
        self._timer_thread.daemon = True
        self._timer_thread.start()

    def _update_timer(self):
        while True:
            time.sleep(self._EXPIRES_SECONDS)
            self._update_thread = threading.Thread(target=self._update)
            self._update_thread.start()

    @staticmethod
    def _build_stops_index(stations):
        stops = {}
        for id in stations:
            for stop_id in stations[id]['stops'].keys():
                stops[stop_id] = stations[id]

        return stops

    @staticmethod
    def _load_mta_feed(feed_url):
        mta_data = None
        try:
            with contextlib.closing(urllib2.urlopen(feed_url)) as r:
                data = r.read()
                return FeedResponse(data)

        except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
            logger.error('Couldn\'t connect to MTA server: ' + str(e))
            return False

    def _update(self):
        if not self._update_lock.acquire(False):
            logger.info('Update locked!')

            lock_age = datetime.datetime.now() - self._update_lock_time
            if lock_age.total_seconds() > self._LOCK_TIMEOUT:
                self._update_lock = threading.Lock()
                logger.info('Cleared expired update lock')

            return

        self._update_lock_time = datetime.datetime.now()
        logger.info('updating...')

        # create working copy for thread safety
        stations = copy.deepcopy(self._stations)

        # clear old times
        for id in stations:
            stations[id]['N'] = []
            stations[id]['S'] = []
            stations[id]['routes'] = set()

        stops = self._build_stops_index(stations)
        routes = defaultdict(set)

        for i, feed_url in enumerate(self._FEED_URLS):
            mta_data = self._load_mta_feed(feed_url)

            if not mta_data:
                continue

            self._last_update = datetime.datetime.fromtimestamp(mta_data.header.timestamp, self._tz)
            self._MAX_TIME = self._last_update + datetime.timedelta(minutes = self._MAX_MINUTES)

            for entity in mta_data.entity:
                trip = Trip(entity)

                if not trip.is_valid():
                    continue

                direction = trip.direction[0]
                route_id = trip.route_id

                # check if this is a trip_update only otherwise skip but why?

                for field in entity.trip_update.trip.ListFields():
                    if field[0].full_name == 'transit_realtime.TripDescriptor.trip_id':
                        trip_id = field[1]
                # added below station['routes'].add(route_id)

                for update in entity.trip_update.stop_time_update:
                    trip_stop = TripStop(update)

                    time = trip_stop.time
                    if time < self._last_update or time > self._MAX_TIME:
                        continue

                    stop_id = trip_stop.stop_id

                    if stop_id not in stops:
                        logger.info('Stop %s not found', stop_id)
                        continue

                    station = stops[stop_id]
                    station['routes'].add(route_id)
                    station[direction].append({
                        #add trip_id
                        'trip_id': trip_id,
                        'route': route_id,
                        'time': time
                    })

                    routes[route_id].add(stop_id)


        # sort by time
        for id in stations:
            if stations[id]['S'] or stations[id]['N']:
                stations[id]['hasData'] = True
                stations[id]['S'] = sorted(stations[id]['S'], key=itemgetter('time'))[:self._MAX_TRAINS]
                stations[id]['N'] = sorted(stations[id]['N'], key=itemgetter('time'))[:self._MAX_TRAINS]
            else:
                stations[id]['hasData'] = False

        with self._read_lock:
            self._stops = stops
            self._routes = routes
            self._stations = stations

        self._update_lock.release()

    def last_update(self):
        return self._last_update

    def get_by_point(self, point, limit=5):
        if self.is_expired():
            self._update()

        with self._read_lock:
            sortable_stations = copy.deepcopy(self._stations)

        sortable_stations.sort(key=lambda x: distance(x['location'], point))
        return sortable_stations[:limit]

    def get_routes(self):
        return self._routes.keys()

    def get_by_route(self, route):
        if self.is_expired():
            self._update()

        with self._read_lock:
            out = [ self._stops[k] for k in self._routes[route] ]

        out.sort(key=lambda x: x['name'])

        return out


    def get_by_station(self, ids):
        if self.is_expired():
            self._update()
#        out =  [self._stops[str(k)] for k in ids]
        with self._read_lock:
            out = dict()
            for k in ids:
                if(k in self._stops.keys()):
                    out[str(k)] = self._stops[str(k)]
        return out

    def get_by_id(self, ids):
        if self.is_expired():
            self._update()

        with self._read_lock:
            #//this gets it by a station number not by stop id
            out = [ self._stations[k] for k in ids ]

        return out

    def is_expired(self):
        if self._THREADED:
            # check that the update thread is still running
            if not self._timer_thread.is_alive():
                self._start_timer()
                return False

        elif self._EXPIRES_SECONDS:
            age = datetime.datetime.now(self._tz) - self._last_update
            return age.total_seconds() > self._EXPIRES_SECONDS
        else:
            return False
