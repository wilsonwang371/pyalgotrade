import enum
import sys
import pyalgotrade.fsm as fsm
import pyalgotrade.logger


logger = pyalgotrade.logger.getLogger('strategyfsm')


class SampleStrategyFSMState(enum.Enum):

    INIT = 1
    STATE1 = 2
    STATE2 = 3
    ERROR = 99


class SampleStrategyFSM(fsm.StateMachine):

    def __init__(self, barfeed):
        super(SampleStrategyFSM, self).__init__()
        self.__barfeed = barfeed

    @fsm.state(SampleStrategyFSMState.INIT, True)
    def state_init(self, bars):
        logger.info('INIT')
        return SampleStrategyFSMState.STATE1
    
    @fsm.state(SampleStrategyFSMState.STATE1, False)
    def state_state1(self, bars):
        logger.info('STATE1')
        return SampleStrategyFSMState.STATE2

    @fsm.state(SampleStrategyFSMState.STATE2, False)
    def state_state2(self, bars):
        logger.info('STATE2')
        return SampleStrategyFSMState.ERROR
    
    @fsm.state(SampleStrategyFSMState.ERROR, False)
    def state_error(self, bars):
        logger.info('ERROR')
        sys.exit(0)
