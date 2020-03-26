import coloredlogs
import pyalgotrade.logger
from pyalgotrade.apps.utils.muxplugin import MuxPlugin

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

DIFF_THRESHOLD = 2.0

class PrintChangeMuxPlugin(MuxPlugin):

    def __init__(self):
        self.lastdiff = None

    def process(self, data):
        keys = list(data.keys())
        assert len(keys) == 2
        for i in keys:
            if data[i] is None:
                return None
        assert 'close' in data[keys[0]] and 'close' in data[keys[1]]
        close1 = data[keys[0]]['close']
        close2 = data[keys[1]]['close']
        diff = abs(close1 - close2)
        if self.lastdiff is None or abs(diff - self.lastdiff) > DIFF_THRESHOLD:
            logger.info('Diff {:.2f}: {}[{:.2f}] {}[{:.2f}]'.format(diff, keys[0], close1, keys[1], close2))
            self.lastdiff = diff
        return None
