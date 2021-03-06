import mysql.connector
import time
import yaml
from yaml import CLoader as Loader
with open('config.yaml', 'r') as f:
    config = yaml.load(f, Loader=Loader)['DB']


class DB():
    def __init__(self):
        self.cnx = mysql.connector.connect(**config)
        self.cursor = self.cnx.cursor(prepared=True)

    def get_parsed_values(self, project_id: int, start_time: int) -> list:
        self.cursor.execute(
            'SELECT projects.filters, price, timestamp, projects.chain FROM floor_price INNER JOIN projects ON floor_price.project_id=projects.id WHERE project_id=(?) AND timestamp>=(?) ORDER BY timestamp ASC',
            (project_id, start_time))
        data = self.cursor.fetchall()

        return data

    def get_projects(self) -> list:
        self.cursor.execute('SELECT * FROM projects')
        data = self.cursor.fetchall()

        return data

    def get_all_ids(self) -> list:
        ids = []

        self.cursor.execute('SELECT id FROM projects')
        data = self.cursor.fetchall()
        for i in data:
            ids.append(i[0])


        return ids

    def __get_max_value(self) -> int:
        self.cursor.execute('SELECT max(id) FROM projects')
        data = self.cursor.fetchall()

        return int(data[0][0])

    def insert_parsed_values(self, project_id: int, price: float, timestamp: int) -> None:
        self.cursor.execute('INSERT INTO floor_price(project_id,price,timestamp) VALUES (?,?,?)',
                            (project_id, price, timestamp))
        self.cnx.commit()

    def insert_project(self, filters: str, chain: str) -> int:
        create_time = int(time.time())
        self.cursor.execute('INSERT INTO projects(filters,chain create_time) VALUES (?,?,?)', (filters, chain, create_time))
        self.cnx.commit()

        return self.__get_max_value()

if __name__ == '__main__':
    db = DB()
    ids=db.get_all_ids()
    print(ids)


