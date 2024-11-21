# Copyright (c) 2023-2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    datamodel.py
# @author  Michael Behrisch
# @date    2023-01-11

import datetime
from typing import Optional

from sqlalchemy import Integer, DateTime, ForeignKey, String, BigInteger, SmallInteger, TypeDecorator, inspect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

HISTOGRAM_0_120PLUS_5KMH = 1
INTERVALS = {"hourly": 3600, "quarterly": 900}

IntID = BigInteger().with_variant(Integer, "sqlite")


class TZDateTime(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_result_value(self, value, dialect):
        if value is not None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value


def parse_utc(date):
    if isinstance(date, datetime.datetime):
        return date.astimezone(datetime.timezone.utc)
    return datetime.datetime.fromisoformat(date.replace("Z", "+00:00")) if date else None


class Base(DeclarativeBase):
    pass


class TrafficCount(Base):
    __tablename__ = "traffic_count"

    id: Mapped[int] = mapped_column(IntID, primary_key=True)
    instance_id: Mapped[Optional[int]] = mapped_column(IntID)
    segment_id: Mapped[int] = mapped_column(IntID, ForeignKey("segment.id"))
    date_utc: Mapped[datetime.datetime] = mapped_column(TZDateTime)
    interval_seconds: Mapped[Optional[int]]
    uptime_rel: Mapped[float]
    heavy_lft: Mapped[float]
    heavy_rgt: Mapped[float]
    car_lft: Mapped[float]
    car_rgt: Mapped[float]
    bike_lft: Mapped[float]
    bike_rgt: Mapped[float]
    pedestrian_lft: Mapped[float]
    pedestrian_rgt: Mapped[float]
    direction: Mapped[int] = mapped_column(SmallInteger)
    v85: Mapped[Optional[float]]
    car_speed_histogram_type: Mapped[int] = mapped_column(SmallInteger)
    car_speed_histogram: Mapped[str] = mapped_column(String(length=250))
    detail: Mapped[str] = mapped_column(String(length=10))  # one of "basic", "advanced"

    __mapper_args__ = {
        "polymorphic_identity": "basic",
        "polymorphic_on": "detail",
    }

    def __init__(self, table):
        super().__init__()
        for attr, val in table.items():
            if hasattr(self, attr):
                setattr(self, attr, None if val == -1 else val)
        self.date_utc = parse_utc(table["date"])
        self.interval_seconds = INTERVALS.get(table["interval"])
        self.uptime_rel = table["uptime"]
        speed_hist = table.get("car_speed_hist_0to120plus")
        if speed_hist:
            self.car_speed_histogram_type = HISTOGRAM_0_120PLUS_5KMH
            last_idx = max([i for i, v in enumerate(speed_hist) if v > 0] + [0])
            counts = [round(v / 100 * self._unscaled_car_count()) for v in speed_hist[:last_idx+1]]
            self.car_speed_histogram = ",".join(["%s" % c for c in counts])

    def _unscaled_car_count(self):
        return round((self.car_lft + self.car_rgt) * self.uptime_rel)

    def get_histogram(self):
        result = []
        if self.car_speed_histogram_type == HISTOGRAM_0_120PLUS_5KMH:
            counts = [float(f) for f in self.car_speed_histogram.split(",")]
            ucc = self._unscaled_car_count()
            for low in range(0, 80, 10):
                high = low + 10 if low < 70 else 1000
                c = 0.
                for i, f in enumerate(counts):
                    if low <= 5 * i and 5 * (i + 1) <= high:
                        c += f
                result.append(100 * c / ucc if ucc != 0. else 0.)
        return result

    @classmethod
    def modes(cls):
        return ["pedestrian", "bike", "car", "heavy"]


class TrafficCountAdvanced(TrafficCount):
    __tablename__ = "traffic_count_advanced"

    id: Mapped[int] = mapped_column(ForeignKey("traffic_count.id"), primary_key=True)
    mode_bicycle_lft: Mapped[float]
    mode_bicycle_rgt: Mapped[float]
    mode_bus_lft: Mapped[float]
    mode_bus_rgt: Mapped[float]
    mode_car_lft: Mapped[float]
    mode_car_rgt: Mapped[float]
    mode_lighttruck_lft: Mapped[float]
    mode_lighttruck_rgt: Mapped[float]
    mode_motorcycle_lft: Mapped[float]
    mode_motorcycle_rgt: Mapped[float]
    mode_pedestrian_lft: Mapped[float]
    mode_pedestrian_rgt: Mapped[float]
    mode_stroller_lft: Mapped[float]
    mode_stroller_rgt: Mapped[float]
    mode_tractor_lft: Mapped[float]
    mode_tractor_rgt: Mapped[float]
    mode_trailer_lft: Mapped[float]
    mode_trailer_rgt: Mapped[float]
    mode_truck_lft: Mapped[float]
    mode_truck_rgt: Mapped[float]
    mode_night_lft: Mapped[float]
    mode_night_rgt: Mapped[float]

    __mapper_args__ = {
        "polymorphic_identity": "advanced",
    }

    @classmethod
    def modes(cls):
        return TrafficCount.modes() + [a[:-4] for a in inspect(cls).columns.keys() if a.startswith("mode_") and a.endswith("_lft")]


# the following line is due to a pylint bug which complains about the Mapped[] otherwise
# pylint: disable=unsubscriptable-object
class Segment(Base):
    __tablename__ = "segment"

    id: Mapped[int] = mapped_column(IntID, primary_key=True, autoincrement=False)
    last_data_utc: Mapped[datetime.datetime] = mapped_column(TZDateTime)
    last_backup_utc: Mapped[datetime.datetime] = mapped_column(TZDateTime)
    timezone: Mapped[str] = mapped_column(String(length=50))

    cameras = relationship("Camera")
    counts = relationship("TrafficCount")

    def __init__(self, properties):
        self.id = properties["segment_id"]
        self.last_data_utc = parse_utc(properties.get("last_data_package"))
        self.timezone = properties["timezone"]

    def add_camera(self, table):
        self.cameras.append(Camera(table))

    def update(self, properties):
        self.last_data_utc = parse_utc(properties.get("last_data_package"))
        self.timezone = properties["timezone"]


class Camera(Base):
    __tablename__ = "camera"

    id: Mapped[int] = mapped_column(IntID, primary_key=True, autoincrement=False)
    mac: Mapped[int] = mapped_column(IntID)
    user_id: Mapped[int] = mapped_column(IntID)
    segment_id: Mapped[int] = mapped_column(IntID, ForeignKey("segment.id"))
    direction: Mapped[bool]
    status: Mapped[str] = mapped_column(String(length=20))  # probably one of "active", "non_active", "problematic", "stopped"
    manual: Mapped[bool]
    added_utc: Mapped[datetime.datetime] = mapped_column(TZDateTime)
    end_utc: Mapped[Optional[datetime.datetime]] = mapped_column(TZDateTime)
    last_data_utc: Mapped[Optional[datetime.datetime]] = mapped_column(TZDateTime)
    first_data_utc: Mapped[Optional[datetime.datetime]] = mapped_column(TZDateTime)
    pedestrians_left: Mapped[bool]
    pedestrians_right: Mapped[bool]
    bikes_left: Mapped[bool]
    bikes_right: Mapped[bool]
    cars_left: Mapped[bool]
    cars_right: Mapped[bool]
    is_calibration_done: Mapped[str] = mapped_column(String(length=10))  # probably one of "yes", "no", "partial"
    hardware_version: Mapped[int]

    def __init__(self, table):
        super().__init__()
        for attr, val in table.items():
            if hasattr(self, attr):
                setattr(self, attr, None if val == -1 else val)
        self.id = table["instance_id"]
        self.added_utc = parse_utc(table["time_added"])
        self.end_utc = parse_utc(table["time_end"])
        self.last_data_utc = parse_utc(table["last_data_package"])
        self.first_data_utc = parse_utc(table["first_data_package"])
