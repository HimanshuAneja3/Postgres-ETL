import psycopg2
from tableSchema import drop_table_list, create_table_list


def createDatabase():
    conn = psycopg2.connect(
        database=POSTGRESDB,
        user=POSTGRESDB_USERNAME,
        host="localhost",
        password=POSTGRESDB_PASSWORD,
        port=5432,
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS postgresetl")
    cur.execute("CREATE DATABASE postgresetl WITH ENCODING 'utf8' TEMPLATE template0")
    conn.close()
    conn1 = psycopg2.connect(
        database=POSTGRESDB,
        user=POSTGRESDB_USERNAME,
        host="localhost",
        password=POSTGRESDB_PASSWORD,
        port=5432,
    )
    cur1 = conn1.cursor()
    return cur1, conn1


def dropTables(cur1, conn1):
    """
    Drops each table using the queries in drop_table_list list.
    """
    for droplist in drop_table_list:
        cur1.execute(droplist)
        conn1.commit()


def createTables(cur1, conn1):
    pass
    """
    Creates each table using the queries in create_table_list list.
    """
    for createquery in create_table_list:
        cur1.execute(createquery)
        conn1.commit()


def main():
    cur1, conn1 = createDatabase()
    dropTables(cur1, conn1)
    createTables(cur1, conn1)
    conn1.close()


if __name__ == "__main__":
    main()
