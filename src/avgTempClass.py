class AvgTempClass:
    def __init__(self, date):
        self.Date = date
        self.DaysCount = 0
        self._TemMinAcco = 0.0
        self._TemMaxAcco = 0.0

    def addTemValues(self, temMin, temMax):
        self._TemMinAcco = self._TemMinAcco + temMin
        self._TemMaxAcco = self._TemMaxAcco + temMax
        self.DaysCount = self.DaysCount + 1

    def getMinAvgTemp(self):
        if (self.DaysCount > 0):
            return self._TemMinAcco/self.DaysCount
        else:
            return 

    def getMaxAvgTemp(self):
        if (self.DaysCount > 0):
            return self._TemMaxAcco/self.DaysCount
        else:
            return

    def getOneRowInfo(self, stringSeparator):
        sep = str(stringSeparator)
        dayCount = str(self.DaysCount)
        date = self.Date
        if (self.DaysCount > 0):
            minAveTemp = str(self.getMinAvgTemp())
            maxAveTemp = str(self.getMaxAvgTemp())
            line = sep.join([date, dayCount, minAveTemp, maxAveTemp])
        else:
            line = sep.join([date, dayCount])
        return line
