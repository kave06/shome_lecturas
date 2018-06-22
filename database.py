import psycopg2
import config
from datetime import datetime


def get_counters() -> list:
    counters = """ 
                SELECT * 
                FROM energy_counter
                ORDER BY id ASC 
            """
    counters = get_query(counters)
    counters2 = []
    for row in counters:
        if row[1] == 'distribution':
            counters2.append(row)
    return counters2


def get_query(query: str) -> list:
    conn = None
    result_set = []

    try:
        conn = psycopg2.connect(host=config.host, database=config.database,
                                user=config.user, password=config.password)
        cur = conn.cursor()
        cur.execute(query)

        rows = cur.fetchall()
        # result_set.append(cur.rowcount)
        for row in rows:
            result_set.append(row)

        cur.close()
        return result_set
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_value_stream(stream_id) -> float:
    query = """
            SELECT CAST(value AS DOUBLE PRECISION) as requested
            FROM iotgate.node_component_state
            WHERE stream_id = {} 
            ORDER BY observed_at DESC
            LIMIT 1
            """.format(stream_id)
    list = get_query(query)
    requested = list[0][0]
    return requested


def get_last_official_close():
    query = """
                SELECT * 
                FROM official_close
                ORDER BY init_date DESC
                LIMIT 1
            """
    value = get_query(query)
    value = value[0][0]
    return value


def get_last_official_close_read():
    query = """
                SELECT id 
                FROM official_close_read
                ORDER BY id DESC
                LIMIT 1
            """
    value = get_query(query)
    value = value[0][0]
    return value


def create_list_official_close_read() -> list:
    official_close_read = []
    counters2 = get_counters()
    last_official_close = get_last_official_close()
    print('last_official close: {}'.format(last_official_close))
    last_official_close_read = get_last_official_close_read()

    requested_list = []
    for counter in counters2:
        stream_id = counter[4]
        requested_list.append(get_value_stream(stream_id))

    for j in range(len(counters2)):
        official_close_read_tmp = []
        # id
        official_close_read_tmp.append(last_official_close_read + 1 + j)
        # requested, value
        official_close_read_tmp.append(requested_list[j])
        # official close id
        official_close_read_tmp.append(last_official_close)
        # phy_counter_id
        official_close_read_tmp.append(counters2[j][3])
        # measure
        if counters2[j][2] == 1:
            official_close_read_tmp.append('AguaFria')
        elif counters2[j][2] == 2:
            official_close_read_tmp.append('ACS')
        elif counters2[j][2] == 3:
            official_close_read_tmp.append('HoneywellEW7730')
        elif counters2[j][2] == 4:
            official_close_read_tmp.append('Calefaccion/Refrigracion')
        elif counters2[j][2] == 5:
            official_close_read_tmp.append('Calefaccion/Refrigracion')
        official_close_read.append(official_close_read_tmp)

    return official_close_read


def insert_tables_official_close():
    last = []
    last2 = []
    last_official_close = get_last_official_close()
    # official close id
    last.append(last_official_close + 1)
    # init date
    last.append(datetime.now())
    # previous official close id
    last.append(last_official_close)
    last2.append(last)
    query = "INSERT INTO official_close VALUES (%s, %s, %s)"
    # query = "INSERT INTO official_close VALUES (53, current_timestamp, 52)"
    insert_table(query, last2)
    official_close_read = create_list_official_close_read()

    query2 = "INSERT INTO official_close_read (id, requested, official_close_id," \
             "phy_counter_id, measuring ) VALUES (%s, %s, %s, %s, %s)"
    insert_table(query2, official_close_read)


def insert_table(query: str, values: list):
    conn = None

    try:
        conn = psycopg2.connect(host=config.host, database=config.database,
                                user=config.user, password=config.password)
        cur = conn.cursor()
        cur.executemany(query, values)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_list_requested():
    counters = get_counters()
    last_official_close = get_last_official_close()
    print('last_official close: {}'.format(last_official_close))

    requested_list = []
    for counter in counters:
        stream_id = counter[4]
        requested_list.append(get_value_stream(stream_id))

    return requested_list


def get_list_to_update_official_close_read():
    query = """
        SELECT tmp.valueMonth,
            energy_counter.phy_counter_id,
            tmp.month,
            tmp.stream_id
        FROM
            energy_counter,
            (
            SELECT 
                stream_id, 
                EXTRACT(MONTH FROM observed_at) AS month, 
                MAX(value) as valueMonth
            FROM iotgate.node_component_state, energy_counter
            WHERE EXTRACT(MONTH FROM observed_at)=6 AND stream_id iN
                (
                SELECT iotgate_component_id
                FROM energy_counter
                WHERE iotgate_component_id IN
                    (
                    SELECT iotgate_component_id
                    FROM energy_counter
                    WHERE type='distribution'
                    )
                )
            GROUP BY stream_id, EXTRACT(MONTH FROM observed_at)
            ) AS tmp
        WHERE energy_counter.iotgate_component_id=tmp.stream_id
            """

    return get_query(query)


def update_values_official_close_read():
    query = """
        UPDATE official_close_read
        SET requested = %s
        WHERE phy_counter_id = %s AND
              official_close_id = %s
            """
    updates = get_list_to_update_official_close_read()

    for update in updates:
        update_table(query, (update[0], update[1], 78))


def update_table(query: str, values: list):
    conn = None

    try:
        conn = psycopg2.connect(host=config.host, database=config.database,
                                user=config.user, password=config.password)
        cur = conn.cursor()
        cur.execute(query, values)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# official_close_read = create_list_official_close_read()
# print(official_close_read)

# for row in official_close_read:
#     print('{}\t{}\t\t{}\t{}\t{}'.format(row[0], row[1], row[2], row[3], row[4]))

# insert_tables_official_close()

# print(get_last_official_close_read())

# insert_tables_official_close()
# lista = get_list_requested()
# print(len(lista))


# list = get_list_to_update_official_close_read()
# print(list)

# update_values_official_close_read()
list = get_list_requested()
print(list)