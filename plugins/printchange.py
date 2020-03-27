import coloredlogs
import pyalgotrade.logger
from pyalgotrade.apps.utils.plugin import Plugin

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

class PrintChangeMuxPlugin(Plugin):

    def __init__(self, *args):
        pass

    def process(self, key, data):
        logger.info('Data updated: {} {}'.format(key, data))
        return None
