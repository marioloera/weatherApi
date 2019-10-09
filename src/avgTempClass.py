class AvgTempClass:
    def __init__(self, date):
        self.date = date
        self.daysCount = 0
        self._minTemAcco = 0.0
        self._maxTemAcco = 0.0

    def addTemValues(self, minTem, maxTem):
        self._minTemAcco = self._minTemAcco + minTem
        self._maxTemAcco = self._maxTemAcco + maxTem
        self.daysCount = self.daysCount + 1

    def getMinAvgTemp(self):
        if (self.daysCount > 0):
            return self._minTemAcco/self.daysCount
        else:
            return 

    def getMaxAvgTemp(self):
        if (self.daysCount > 0):
            return self._maxTemAcco/self.daysCount
        else:
            return

    def getOneRowInfo(self, stringSeparator):
        sep = str(stringSeparator)
        dayCount = str(self.daysCount)
        date = self.date
        if (self.daysCount > 0):
            minAveTemp = str(self.getMinAvgTemp())
            maxAveTemp = str(self.getMaxAvgTemp())
            line = sep.join([date, dayCount, minAveTemp, maxAveTemp])
        else:
            line = sep.join([date, dayCount])
        return line
