import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ChartOverlay(ABC):

    def __init__(self, enabled=True):
        self.enabled = enabled

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def apply(self, fig, df):
        pass

    def is_available(self):
        return True
