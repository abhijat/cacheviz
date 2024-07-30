import re
from dataclasses import dataclass
from typing import TextIO


@dataclass
class CacheStats:
    current_items_size: int
    reserved_size: int
    pending_reservations_size: int
    max_bytes: int
    disk_size: int
    mem_trim_status: bool
    disk_trim_status: bool
    puts_pending: int
    mem_trim_target: int
    disk_trim_target: int
    mem_trimmed: int
    disk_trimmed: int


class LogParser:
    GROUND = re.compile(r"Cache max_bytes adjusted to (?P<max_bytes>\d+).*Disk size (?P<disk_size>\d+).*")
    RES = re.compile(r"reserve_space: reserved (?P<res_bytes>\d+)/\d+ bytes/objects")
    REL = re.compile(r"reserve_space_release: releasing (?P<rel_bytes>\d+)/\d+ reserved bytes/objects")
    RECL = re.compile(r"trim: reclaimed\(fast\) (?P<recl>\d+) bytes from ")
    MEMT = re.compile(r"in-memory trim: set target_size (?P<target_size>\d+)/\d+, size \d+/\d+")
    DISKT = re.compile(r"- trim: set target_size (?P<target_size>\d+)/\d+, size \d+/\d+")

    def __init__(self, handle: TextIO):
        self.handle = handle
        self.stats = CacheStats(current_items_size=0, reserved_size=0, pending_reservations_size=0, max_bytes=0,
                                disk_size=0, mem_trim_status=False, disk_trim_status=False, puts_pending=0,
                                mem_trim_target=0, disk_trim_target=0, mem_trimmed=0, disk_trimmed=0)
        self.depleted_file = False

    def start(self):
        while self.stats.max_bytes == 0:
            self.next()

    def next(self) -> str:
        if self.depleted_file:
            return ""

        row = ""
        while not self.depleted_file and "cache_service.cc" not in row:
            row = self.handle.readline()
            if not row:
                self.depleted_file = True

        if "cache_service.cc" in row:
            row = row.strip()
            if "Cache max_bytes adjusted" in row:
                self.update_ground_values(row)
            if "reserve_space: reserved " in row:
                self.update_reserved(row)
            if "reserve_space_release: releasing " in row:
                self.update_reserve_released(row)
                self.stats.puts_pending -= 1
            if "trim: reclaimed(fast) " in row:
                self.update_reclaimed_space(row)
            if "in-memory trim: set target_size " in row:
                self.stats.mem_trim_status = True
                self.update_mem_trim_target(row)
            if "in-memory trim result: " in row:
                self.stats.mem_trim_status = False
                self.stats.mem_trimmed = 0
            if "in-memory" not in row:
                if "trim: set target_size " in row:
                    self.stats.disk_trim_status = True
                    self.update_disk_trim_target(row)
                if "trim: deleted " in row:
                    self.stats.disk_trim_status = False
                    self.stats.disk_trimmed = 0
            if "Trying to put" in row:
                self.stats.puts_pending += 1
        return row

    def update_ground_values(self, row: str):
        match = self.GROUND.search(row)
        self.stats.max_bytes = int(match.group("max_bytes"))
        self.stats.disk_size = int(match.group("disk_size"))

    def update_reserved(self, row: str):
        match = self.RES.search(row)
        self.stats.reserved_size += int(match.group("res_bytes"))

    def update_reserve_released(self, row: str):
        match = self.REL.search(row)
        bytes_released = int(match.group("rel_bytes"))
        self.stats.reserved_size -= bytes_released
        self.stats.current_items_size += bytes_released

    def update_reclaimed_space(self, row: str):
        match = self.RECL.search(row)
        self.stats.current_items_size -= int(match.group("recl"))
        if self.stats.mem_trim_status:
            self.stats.mem_trimmed += int(match.group("recl"))
        if self.stats.disk_trim_status:
            self.stats.disk_trimmed += int(match.group("recl"))

    def update_mem_trim_target(self, row: str):
        match = self.MEMT.search(row)
        self.stats.mem_trim_target = self.stats.max_bytes - int(match.group("target_size"))

    def update_disk_trim_target(self, row: str):
        match = self.DISKT.search(row)
        self.stats.disk_trim_target = self.stats.max_bytes - int(match.group("target_size"))
