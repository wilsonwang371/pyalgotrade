import coloredlogs
import pyalgotrade.logger
from pyalgotrade.apps.muxplugins import MuxPlugin

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

class EmptyMuxPlugin(MuxPlugin):

    def process(self, data):
        return None
