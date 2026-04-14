import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional

logger = logging.getLogger(__name__)


class PatternType(Enum):
    W_BOTTOM = "W_BOTTOM"
    M_TOP = "M_TOP"
    HEAD_SHOULDER_BOTTOM = "HEAD_SHOULDER_BOTTOM"
    HEAD_SHOULDER_TOP = "HEAD_SHOULDER_TOP"
    BREAKOUT_FAIL = "BREAKOUT_FAIL"
    PODIE_FAN = "PODIE_FAN"
    FLAG_DOWN = "FLAG_DOWN"
    FLAG_UP = "FLAG_UP"
    TRIANGLE_BOTTOM = "TRIANGLE_BOTTOM"
    TRIANGLE_HEAD = "TRIANGLE_HEAD"
    ASCENDING_TRIANGLE = "ASCENDING_TRIANGLE"
    DESCENDING_TRIANGLE = "DESCENDING_TRIANGLE"


class PatternStatus(Enum):
    FORMING = "forming"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"


class PatternDirection(Enum):
    BULLISH = "bull"
    BEARISH = "bear"


@dataclass
class KeyPoint:
    name: str
    date: object
    price: float
    bi_index: int


@dataclass
class PatternResult:
    ts_code: str = ""
    period: str = "daily"
    pattern_type: Optional[PatternType] = None
    direction: Optional[PatternDirection] = None
    status: PatternStatus = PatternStatus.FORMING
    start_date: object = None
    end_date: object = None
    neckline_price: float = 0.0
    neckline_slope: float = 0.0
    key_points: List[KeyPoint] = field(default_factory=list)
    strength_score: float = 0.0
    volume_score: float = 0.0
    target_price: float = 0.0
    stop_loss_price: float = 0.0
    bi_indices: List[int] = field(default_factory=list)
    confidence: float = 0.0
    update_time: str = ""


class BasePatternDetector(ABC):

    @abstractmethod
    def detect(self, bis: list, kline_df) -> list:
        pass

    @property
    def required_bi_count(self) -> int:
        return 4
