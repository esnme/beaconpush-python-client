# coding=UTF-8
class BeaconpushEvent(object):
    def __init__(self, operatorId, name, userId, channel):
        self.operatorId = operatorId
        self.name = name
        self.userId = userId
        self.channel = channel

    def __repr__(self):
        return "BeaconpushEvent %s for %s: user:%s channel:%s" % (self.name, self.operatorId, self.userId, self.channel)

def createEvent(args):
    return BeaconpushEvent(args[0], args[1], args[2], args[3])
