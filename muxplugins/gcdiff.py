import six

import coloredlogs
import pyalgotrade.logger
from pyalgotrade.apps.utils.muxplugin import MuxPlugin

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

DIFF_THRESHOLD = 2.0

class PrintChangeMuxPlugin(MuxPlugin):

    def __init__(self):
        self.lastdiff = None
        self.values = {}

    def process(self, key, data):
        keys = []
        closevals = []
        tsvals = []
        self.values[key] = data
        if len(self.values) != 2:
            return None
        for k, v in six.iteritems(self.values):
            assert v is not None
            assert 'close' in v
            keys.append(k)
            tsvals.append(v['timestamp'])
            closevals.append(v['close'])
        assert len(closevals) == 2
        diff = abs(closevals[0] - closevals[1])
        if self.lastdiff is None or abs(diff - self.lastdiff) > DIFF_THRESHOLD:
            logger.info('Diff {:.2f}: {}[{:.2f}] {}[{:.2f}]'.format(diff,
                keys[0], closevals[0],
                keys[1], closevals[1],))
            self.lastdiff = diff
        res = {
            keys[0]: closevals[0],
            keys[1]: closevals[1],
            'diff': diff,
            'timestamp': min(tsvals)}
        return res
