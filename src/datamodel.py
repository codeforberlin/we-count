# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    datamodel.py
# @author  Michael Behrisch
# @date    2023-01-11

import datetime
from fractions import Fraction

from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey, Boolean, String, BigInteger, SmallInteger, TypeDecorator
from sqlalchemy.orm import declarative_base, relationship

HISTOGRAM_0_120PLUS_5KMH = 1

Base = declarative_base()
IntID = BigInteger().with_variant(Integer, "sqlite")


class TZDateTime(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_result_value(self, value, dialect):
        if value is not None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value


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
        self.car_speed_histogram_type = HISTOGRAM_0_120PLUS_5KMH
        speed_hist = table["car_speed_hist_0to120plus"]
        last_idx = max([i for i, v in enumerate(speed_hist) if v > 0] + [0])
        fracs = [Fraction(v / 100).limit_denominator(9999) for v in speed_hist[:last_idx+1]]
        self.car_speed_histogram = ",".join(["%s/%s" % (f.numerator, f.denominator) if f.denominator != 1 else str(f.numerator) for f in fracs])
    
    id = Column(IntID, primary_key=True)
    instance_id = Column(IntID)
    segment_id = Column(IntID, ForeignKey("segment.id"))
    date_utc = Column(TZDateTime)
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
    car_speed_histogram_type = Column(SmallInteger)
    car_speed_histogram = Column(String(length=250))

    def get_column_names(self):
        res = ["segment_id", "date_local", "uptime"]
        for mode in ("ped", "bike", "car", "heavy"):
            res += [mode + "_lft", mode + "_rgt", mode + "_total"]
        res += ["v85"]
        return res + ["car_speed%s" % s for s in range(0, 80, 10)]

    def get_column_values(self, tz):
        result = [self.segment_id, str(self.date_utc.astimezone(tz))[:-9],
                  round(self.uptime_rel, 6)]
        for mode in ("pedestrian", "bike", "car", "heavy"):
            lft = round(getattr(self, mode + "_lft"))
            rgt = round(getattr(self, mode + "_rgt"))
            result += [lft, rgt, lft + rgt]
        result += [self.v85]
        if self.car_speed_histogram_type == HISTOGRAM_0_120PLUS_5KMH:
            fracs = [Fraction(f) for f in self.car_speed_histogram.split(",")]
            for low in range(0, 80, 10):
                high = low + 10 if low < 70 else 1e15
                p = Fraction(0)
                for i, f in enumerate(fracs):
                    if low <= 5 * i and 5 * (i + 1) <= high:
                        p += f
                result.append(round(float(100 * p), 2))
        return result


class Segment(Base):
    __tablename__ = "segment"

    def __init__(self, properties, geometry):
        self.id = properties["segment_id"]
        self.last_data_utc = parse_utc(properties["last_data_package"])
        self.timezone = properties["timezone"]
        for i, p in enumerate(geometry):
            point = SegmentGeometry(lon=p[0], lat=p[1], seq=i)
            self.geom.append(point)

    def add_camera(self, table):
        self.cameras.append(Camera(table))

    def update(self, properties):
        self.last_data_utc = parse_utc(properties["last_data_package"])

    id = Column(IntID, primary_key=True, autoincrement=False)
    last_data_utc = Column(TZDateTime)
    last_backup_utc = Column(TZDateTime)
    timezone = Column(String(length=50))

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
    status = Column(String(length=20))  # probably one of "active", "non_active", "problematic", "stopped"
    manual = Column(Boolean)
    added_utc = Column(TZDateTime)
    end_utc = Column(TZDateTime)
    last_data_utc = Column(TZDateTime)
    first_data_utc = Column(TZDateTime)
    pedestrians_left = Column(Boolean)
    pedestrians_right = Column(Boolean)
    bikes_left = Column(Boolean)
    bikes_right = Column(Boolean)
    cars_left = Column(Boolean)
    cars_right = Column(Boolean)
    is_calibration_done = Column(String(length=10))  # probably one of "yes", "no", "partial"
    hardware_version = Column(Integer)
