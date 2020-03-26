import coloredlogs
import pyalgotrade.logger
from pyalgotrade.apps.utils.muxplugin import MuxPlugin

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

class EmptyMuxPlugin(MuxPlugin):

    def __init__(self, *args):
        pass

    def process(self, key, data):
        return None
