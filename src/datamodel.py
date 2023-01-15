# Copyright (c) 2023 Michael Behrisch
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# SPDX-License-Identifier: MIT

# @file    datamodel.py
# @author  Michael Behrisch
# @date    2023-01-11

import datetime
from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class TrafficCount(Base):
    __tablename__ = "traffic_count"

    def __init__(self, table):
        for attr, val in table.items():
            if hasattr(self, attr):
                setattr(self, attr, None if val == -1 else val)
        self.date_utc = datetime.datetime.fromisoformat(table["date"][:-1])
        self.interval_seconds = 3600 if table["interval"] == "hourly" else None
        self.uptime_rel = table["uptime"]
        for i, v in enumerate(table["car_speed_hist_0to120plus"]):
            if v > 0:
                s_low = 5 * i
                s_high = 1000 if s_low == 120 else 5 * (i + 1)
                hist = SpeedHistogram(low_kmh=s_low, up_kmh=s_high, percent=v)
                self.car_speed_hist.append(hist)
    
    id = Column(Integer, primary_key=True)
    instance_id = Column(Integer)
    segment_id = Column(Integer)
    date_utc = Column(DateTime)
    interval_seconds = Column(Integer)
    uptime_rel = Column(Float)
    heavy_lft = Column(Float)
    heavy_rgt = Column(Float)
    car_lft = Column(Float)
    car_rgt = Column(Float)
    bike_lft = Column(Float)
    bike_rgt = Column(Float)
    pedestrian_lft = Column(Float)
    pedestrian_rgt = Column(Float)
    direction = Column(Integer)
    v85 = Column(Float)

    car_speed_hist = relationship("SpeedHistogram")


class SpeedHistogram(Base):
    __tablename__ = "speed_hist"

    id = Column(Integer, primary_key=True)
    traffic_count_id = Column(Integer, ForeignKey("traffic_count.id"))
    low_kmh = Column(Integer)
    up_kmh = Column(Integer)
    percent = Column(Float)
