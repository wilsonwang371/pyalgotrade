import coloredlogs
import pyalgotrade.logger
from pyalgotrade.apps.utils.muxplugin import MuxPlugin

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

class PrintChangeMuxPlugin(MuxPlugin):

    def process(self, key, data):
        logger.info('Data updated: {}'.format(data))
        return None
