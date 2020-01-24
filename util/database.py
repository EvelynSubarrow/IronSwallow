#!/usr/bin/env python3

import json, os, sys, argparse
from collections import Counter, OrderedDict

import psycopg2, psycopg2.extras

from . import config

class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.executed = []

    def __enter__(self) -> DatabaseConnection:
        self.connect()
        return self

    def __exit__(self, type, value, traceback) -> bool:
        self.connection.close()
        return False

    def connect(self) -> psycopg2.connection:
        self.connection = psycopg2.connect(config.get("database-string"))
        return self.connection

    def new_cursor(self) -> psycopg2.cursor:
        return self.connection.cursor()

    def execute_once(self, query) -> None:
        if query not in self.executed:
            with self.new_cursor() as c:
                c.execute(query)
                self.executed.append(query)

