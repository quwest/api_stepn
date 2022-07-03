import json

class CandleData():
    def __init__(self, data):
        self.data = self.__make_dict_from_data(data)
        self.list_with_time = list(self.data.keys())

    def __make_dict_from_data(self, list_data: list) -> dict:
        res_dict = {}
        for i in list_data:
            res_dict[i[1]] = i[0]

        return res_dict

    def __make_periods(self, time_interval_minutes: int) -> list or str:
        interval = {}
        res = []
        try:
            start_time = self.list_with_time[0]
        except IndexError:
            return 'errorid'
        time_interval = time_interval_minutes * 60 * 1100

        for time in self.list_with_time:
            if time < start_time + time_interval:
                interval[time] = round(self.data[time], 4)
            else:
                res.append(interval)
                interval = {}
                interval[time] = round(self.data[time], 4)
                start_time = time
        res.append(interval)

        return res

    def make_candles(self, period: int) -> list or int:
        data_periods = self.__make_periods(period)
        if data_periods=='errorid':
            return 205

        candle = []
        for data_period in data_periods:
            values = data_period.values()
            open_time = list(data_period.keys())[0]
            open = list(values)[0]
            maximum = max(values)
            minimum = min(values)
            close = list(values)[len(values) - 1]

            candle.append([open_time, open, maximum, minimum, close, 0])

        json.dumps(candle)

        return candle
