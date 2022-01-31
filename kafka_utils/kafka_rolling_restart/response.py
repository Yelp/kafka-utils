# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABCMeta, abstractmethod

class BaseRes():
    __metaclass__ = ABCMeta
    @abstractmethod
    def __init__(self, status_code, json, exception=None):
      self.status_code = status_code
      self.json = json
      self.exception = exception

class PrometheusRes(BaseRes):
  """ API for metrics source. Return Prometheus Response.
      :returns Prometheus Response - status_code, json, exception
      :rtype: status_code: int , json: str, exception: Exception
  """
  def __init__(self, status_code, json, exception=None):
    self.status_code = status_code
    self.json = json
    self.exception = exception
    

class JolokiaRes(BaseRes):
  """ API for metrics source. Return Jolokia Response.
      :returns Jolokia Response - status_code, json, exception
      :rtype: status_code: int , json: str, exception: Exception
  """
  def __init__(self, status_code, json, exception=None):
    self.status_code = status_code
    self.json = json
    self.exception = exception