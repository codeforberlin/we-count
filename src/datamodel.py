# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    datamodel.py
# @author  Michael Behrisch
# @date    2023-01-11

import datetime

from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey, Boolean, String, BigInteger, SmallInteger
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()
IntID = BigInteger().with_variant(Integer, "sqlite")


def parse_utc(date):
    return datetime.datetime.fromisoformat(date.replace("Z", "+00:00")) if date else None


class TrafficCount(Base):
    __tablename__ = "traffic_count"

    def __init__(self, table):
        for attr, val in table.items():
            if hasattr(self, attr):
                setattr(self, attr, None if val == -1 else val)
        self.date_utc = parse_utc(table["date"])
        self.interval_seconds = 3600 if table["interval"] == "hourly" else None
        self.uptime_rel = table["uptime"]
        for i, v in enumerate(table["car_speed_hist_0to120plus"]):
            if v > 0:
                s_low = 5 * i
                s_high = 1000 if s_low == 120 else 5 * (i + 1)
                hist = SpeedHistogram(low_kmh=s_low, up_kmh=s_high, percent=v)
                self.car_speed_histogram.append(hist)
    
    id = Column(IntID, primary_key=True)
    instance_id = Column(IntID)
    segment_id = Column(IntID, ForeignKey("segment.id"))
    date_utc = Column(DateTime(True))
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
    direction = Column(SmallInteger)
    v85 = Column(Float)

    def get_column_names(self):
        return list(self.__table__.columns.keys())[2:]  # skip id and instance_id which are not meaningful outside the db

    def get_column_values(self):
        return [getattr(self, attr) for attr in self.get_column_names()]

    car_speed_histogram = relationship("SpeedHistogram")


class SpeedHistogram(Base):
    __tablename__ = "speed_histogram"

    id = Column(IntID, primary_key=True)
    traffic_count_id = Column(IntID, ForeignKey("traffic_count.id"))
    low_kmh = Column(SmallInteger)
    up_kmh = Column(SmallInteger)
    percent = Column(Float)


class Segment(Base):
    __tablename__ = "segment"

    def __init__(self, properties, geometry):
        self.id = properties["segment_id"]
        self.last_data_utc = parse_utc(properties["last_data_package"])
        for i, p in enumerate(geometry):
            point = SegmentGeometry(lon=p[0], lat=p[1], seq=i)
            self.geom.append(point)

    def add_camera(self, table):
        self.cameras.append(Camera(table))

    def update(self, properties):
        self.last_data_utc = parse_utc(properties["last_data_package"])

    id = Column(IntID, primary_key=True, autoincrement=False)
    last_data_utc = Column(DateTime(True))
    last_backup_utc = Column(DateTime(True))

    geom = relationship("SegmentGeometry")
    cameras = relationship("Camera")
    counts = relationship("TrafficCount")


class SegmentGeometry(Base):
    __tablename__ = "segment_geom"

    id = Column(IntID, primary_key=True)
    segment_id = Column(IntID, ForeignKey("segment.id"))
    lon = Column(Float)
    lat = Column(Float)
    seq = Column(SmallInteger)


class Camera(Base):
    __tablename__ = "camera"

    def __init__(self, table):
        for attr, val in table.items():
            if hasattr(self, attr):
                setattr(self, attr, None if val == -1 else val)
        self.id = table["instance_id"]
        self.added_utc = parse_utc(table["time_added"])
        self.end_utc = parse_utc(table["time_end"])
        self.last_data_utc = parse_utc(table["last_data_package"])
        self.first_data_utc = parse_utc(table["first_data_package"])

    id = Column(IntID, primary_key=True, autoincrement=False)
    mac = Column(IntID)
    user_id = Column(IntID)
    segment_id = Column(IntID, ForeignKey("segment.id"))
    direction = Column(Boolean)
    status = Column(String(length=20))  # probably one of "active", "non_active", "problematic"
    manual = Column(Boolean)
    added_utc = Column(DateTime(True))
    end_utc = Column(DateTime(True))
    last_data_utc = Column(DateTime(True))
    first_data_utc = Column(DateTime(True))
    pedestrians_left = Column(Boolean)
    pedestrians_right = Column(Boolean)
    bikes_left = Column(Boolean)
    bikes_right = Column(Boolean)
    cars_left = Column(Boolean)
    cars_right = Column(Boolean)
    is_calibration_done = Column(String(length=10))  # probably either "yes" or "no"
    hardware_version = Column(Integer)
