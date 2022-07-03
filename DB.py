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
            'SELECT projects.section, projects.filters, price, timestamp,chain FROM floor_price INNER JOIN projects ON floor_price.project_id=projects.id WHERE project_id=(?) AND timestamp>=(?) ORDER BY timestamp ASC',
            (project_id, start_time))
        data = self.cursor.fetchall()

        return data

    def get_chain_value(self, project_id: int) -> str:
        self.cursor.execute('SELECT chain FROM floor_price WHERE project_id=(?) LIMIT 1', (project_id,))
        data = self.cursor.fetchall()

        try:
            return data[0][0]
        except IndexError:
            return ''

    def get_project_id(self, section: str, filters: str) -> int:
        data = self.get_projects()
        for i in data:
            if section == i[1] and filters == i[2]:
                return int(i[0])

        project_id = self.insert_project(section, filters)

        return project_id

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

    def insert_parsed_values(self, project_id: int, price: float, timestamp: int, chain: str) -> None:
        self.cursor.execute('INSERT INTO floor_price(project_id,price,timestamp,chain) VALUES (?,?,?,?)',
                            (project_id, price, timestamp, chain))
        self.cnx.commit()

    def insert_project(self, section: str, filters: str) -> int:
        create_time = int(time.time())
        self.cursor.execute('INSERT INTO projects(section, filters, create_time) VALUES (?,?,?)', (section, filters, create_time))
        self.cnx.commit()

        return self.__get_max_value()

if __name__ == '__main__':
    db = DB()
    ids=db.get_all_ids()
    print(ids)


