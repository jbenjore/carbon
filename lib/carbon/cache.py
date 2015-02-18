"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import time
import heapq
import threading
from collections import deque
from functools import total_ordering
from carbon.conf import settings
try:
    from collections import defaultdict
except:
    from util import defaultdict

@total_ordering
class InsertTime(object):
  def __init__(self, metric, when):
    if not when:
      when = time.time()
    self.metric = metric
    self.when = when

  def __eq__(self, other):
    return ((self.metric, self.when) ==
            (other.metric, other.when))

  def __lt__(self, other):
    return ((self.when, self.metric) <
            (other.when, self.metric))

  def __repr__(self):
    return "{metric=%s, when=%s}" % (self.metric, self.when)

class _MetricCache(defaultdict):
  def __init__(self, defaultfactory=deque, method="time", cutoff=300):
    self.method = method
    self.cutoff = cutoff
    self.queue = None
    self.lock = None
    self.insert_time = None
    if self.method == "sorted":
      self.queue = self.gen_queue()
    elif self.method == "time":
      self.insert_time = []
      self.lock = threading.Lock()
    super(_MetricCache, self).__init__(defaultfactory)

  def gen_queue(self):
    while True:
      t = time.time()
      queue = sorted(self.counts, key=lambda x: x[1])
      if settings.LOG_CACHE_QUEUE_SORTS:
        log.debug("Sorted %d cache queues in %.6f seconds" % (len(queue), time.time() - t))
      while queue:
        yield queue.pop()[0]

  @property
  def size(self):
    return reduce(lambda x, y: x + len(y), self.values(), 0)

  def store(self, metric, datapoint):
    try:
      if self.lock:
        self.lock.acquire()
      if self.method == "time" and not self.has_key(metric):
        heapq.heappush(self.insert_time, InsertTime(metric, time.time()))
    self[metric].append(datapoint)
    if self.isFull():
      log.msg("MetricCache is full: self.size=%d" % self.size)
      state.events.cacheFull()
    finally:
      if self.lock:
        self.lock.release()

  def isFull(self):
    # Short circuit this test if there is no max cache size, then we don't need
    # to do the someone expensive work of calculating the current size.
    return settings.MAX_CACHE_SIZE != float('inf') and self.size >= settings.MAX_CACHE_SIZE

  def pop(self, metric=None):
    if not self:
      raise KeyError(metric)
    elif not metric and self.method == "time":
      try:
        if self.lock:
          self.lock.acquire()
        metric_prio = heapq.heappop(self.insert_time)
        metric = metric_prio.metric
        datapoints = (metric, super(_MetricCache, self).pop(metric))
        return datapoints
      finally:
        if self.lock:
          self.lock.release()
    elif not metric and self.method == "max":
      metric = max(self.items(), key=lambda x: len(x[1]))[0]
    elif not metric and self.method == "naive":
      return self.popitem()
    elif not metric and self.method == "sorted":
      metric = self.queue.next()
    datapoints = (metric, super(_MetricCache, self).pop(metric))
    return datapoints

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]

  def has_ready(self):
    if not bool(self):
      return False
    if self.method != "time":
      return True

    cutoff = time.time() - self.cutoff
    min_metric = self.insert_time[0]
    recv_time = min_metric.when
    return recv_time <= cutoff


# Ghetto singleton

MetricCache = _MetricCache(
    method=settings.CACHE_WRITE_STRATEGY,
    cutoff=settings.CACHE_WRITE_BUFFER_TIME
)


# Avoid import circularities
from carbon import log, state
