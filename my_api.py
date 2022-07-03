from datetime import datetime
from flask import jsonify
from flask import Flask
from werkzeug.exceptions import default_exceptions, Aborter
from flask_restful import Api, Resource, reqparse
from convert_data import CandleData
from DB import DB
import werkzeug.exceptions as ex
import json


class UncorrectTimeframe(ex.HTTPException):
    code = 203
    description = 'timeframe should be in :1m, 2m,5m,15m,30m,1h,2h,8h,1d,1w'


class UncorrectId(ex.HTTPException):
    code = 205
    description = f'we do not have this project id, check it in /projects'

class UncorrectDataFormat(ex.HTTPException):
    code = 206
    description = 'uncorrect data format, write it in format dd.mm.yyyy'


default_exceptions[203] = UncorrectTimeframe
default_exceptions[205] = UncorrectId
default_exceptions[206] = UncorrectDataFormat
abort = Aborter()

app = Flask(__name__)
api = Api(app)


class Candle(Resource):
    def __init__(self):
        self.db = DB()
        self.ids=self.db.get_all_ids()

    def get(self, project_id: int, timeframe: int, start_time: str, limit: int):
        if project_id not in self.db.get_all_ids():
            return abort(205)

        try:
            start_time = datetime.strptime(start_time, '%d.%m.%Y')
        except ValueError:
            return abort(206)

        start_time = int(round(start_time.timestamp() * 1000))

        full_data = self.db.get_parsed_values(project_id, start_time)
        price_and_time = [i[2:4] for i in full_data]
        candle = CandleData(price_and_time)

        if timeframe == '1m':
            candle_data = candle.make_candles(1)
        elif timeframe == '2m':
            candle_data = candle.make_candles(2)
        elif timeframe == '5m':
            candle_data = candle.make_candles(5)
        elif timeframe == '15m':
            candle_data = candle.make_candles(15)
        elif timeframe == '30m':
            candle_data = candle.make_candles(30)
        elif timeframe == '1h':
            candle_data = candle.make_candles(60)
        elif timeframe == '2h':
            candle_data = candle.make_candles(120)
        elif timeframe == '4h':
            candle_data = candle.make_candles(240)
        elif timeframe == '8h':
            candle_data = candle.make_candles(480)
        elif timeframe == '1d':
            candle_data = candle.make_candles(1440)
        elif timeframe == '1w':
            candle_data = candle.make_candles(10080)
        else:
            return abort(203)

        return candle_data[0:limit]

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("project_id", nullable=False, type=int, trim=True)
        parser.add_argument("price", nullable=False, type=float, trim=True)
        parser.add_argument("timestamp", nullable=False, type=int, trim=True)
        parser.add_argument("chain", nullable=False, type=str, trim=True)
        params = parser.parse_args()

        self.db.insert_parsed_values(params["project_id"], params["price"], params["timestamp"], params["chain"])

        return params


class Projects(Resource):
    def __init__(self):
        self.db = DB()

    def get(self):
        projects = self.db.get_projects()

        res_dict = {}
        for project in projects:
            res_dict[project[0]] = {'section': project[1]}, \
                                   {'filters': project[2]}, \
                                   {'chain': self.db.get_chain_value(project[0])}, \
                                   {'create_time': [project[3], datetime.fromtimestamp(project[3])]}
        js = jsonify(res_dict)
        return js

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("section", nullable=False, type=str, trim=True)
        parser.add_argument("filters", nullable=False, type=str, trim=True)
        params = parser.parse_args()
        self.db.insert_project(params["section"], params["filters"])

        return params


api.add_resource(Candle, "/candle-data/<int:project_id>/<string:timeframe>/<string:start_time>/<int:limit>",
                 '/candle-data')

api.add_resource(Projects, '/projects', '/projects')

if __name__ == '__main__':
    app.run(debug=True)
