#!/usr/bin/env python3

import sqlalchemy

from . import config


class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.executed = []

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback) -> bool:
        self.connection.close()
        return False

    def connect(self):
        self.engine = sqlalchemy.create_engine(config.get("database-string"))
        self.connection = self.engine.connect().connection


    def new_cursor(self):
        return self.connection.cursor()

    def execute_once(self, query) -> None:
        if query not in self.executed:
            with self.new_cursor() as c:
                c.execute(query)
                self.executed.append(query)

