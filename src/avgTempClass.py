class AvgTempClass:
    def __init__(self, date):
        self.Date = date
        self.DaysCount = 0
        self._TemMinAcco = 0.0
        self._TemMaxAcco = 0.0

    def AddTemValues(self, temMin, temMax):
        self._TemMinAcco = self._TemMinAcco + temMin
        self._TemMaxAcco = self._TemMaxAcco + temMax
        self.DaysCount = self.DaysCount+1

    def GetMinAvgTemp(self):
        if (self.DaysCount > 0):
            return self._TemMinAcco/self.DaysCount
        else:
            return 

    def GetMaxAvgTemp(self):
        if (self.DaysCount > 0):
            return self._TemMaxAcco/self.DaysCount
        else:
            return

    def GetOneRowInfo(self, stringSeparator):
        sep = str(stringSeparator)
        dayCount = str(self.DaysCount)
        date = self.Date
        if (self.DaysCount > 0):
            minAveTemp = str(self.GetMinAvgTemp())
            maxAveTemp = str(self.GetMaxAvgTemp())
            line = sep.join([date, dayCount, minAveTemp, maxAveTemp])
        else:
            line = sep.join([date, dayCount])
        return line
