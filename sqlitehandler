import sqlite3
import logging

def sanity(function):
    def wrapper(*args):
        print(f'\033[93mSainity check {__name__}@{function.__name__}{args}\033[0m')
        try:
            logging.info(f'{function}:{__name__}@{function.__name__}({args})')
            return function(*args)
        except Exception as ex:
            logging.warning(f'{function}:{__name__}@{function.__name__}({args}): FAILED -> {ex}')
            return ex
    return wrapper

#--- connection to database ---#
@sanity 
def connect(db):
    con = sqlite3.connect(db)
    return con

#--- make changes in database ---#
@sanity 
def commit(con, cmd):
    cur = con.cursor()
    cur.execute(cmd)
    res = con.commit()
    return res

#--- search in database ---#
@sanity
def query(con, cmd):
    cur = con.cursor()
    cur.execute(cmd)
    data = cur.fetchall()
    return data

#--- Basic serach ---#
@sanity 
def search(con, table, key, value):
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {key}='{value}'")
    data = cur.fetchall()
    return data

#--- Fuzzy search ---#
@sanity 
def fuzzy_search(con, table, key, value):
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {key} LIKE '{value}'")
    data = cur.fetchall()
    return data

#--- Count elements ---#
@sanity
def count(con, items, table) -> int:
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(DISTINCT {items}) FROM {table}")
    data = cur.fetchall()
    return data[0][0] 

#--- Count elements from key ---#
@sanity
def distinct_count(con, items, table, key, value) -> int:
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(DISTINCT {items}) FROM {table} WHERE {key} LIKE'{value}'")
    data = cur.fetchall()
    return data[0][0] 

#--- load table as dictionary ---#
@sanity
def load_table_dict(con, table):
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table}")
    data = [dict(row) for row in cur.fetchall()]
    return data

#--- distinct search return dictionary ---#
@sanity
def get_as_dict(con, table, key, value):
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {key}='{value}'")
    data = [dict(row) for row in cur.fetchall()]
    return data

#--- fuzzy search return dictionary ---#
@sanity
def get_fuzzy_dict(con, table, key, value):
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {key} LIKE '{value}'")
    data = [dict(row) for row in cur.fetchall()]
    return data

#--- Remove duplicate IDs ---#
@sanity 
def cleanup(con, table, key):
    cmds = [
        f"CREATE TABLE table_temp AS SELECT DISTINCT * FROM {table} WHERE {key} NOT IN (SELECT MIN({key}) FROM {table})",
        f"DROP TABLE {table}",
        f"ALTER TABLE table_temp RENAME TO {table}"]
    for cmd in cmds:
        commit(con, cmd)
    return True
