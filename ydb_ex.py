import os
import ydb
#user_state
# create driver in global space.
drova = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
drova.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(drova)
#path = "user_state"
user_lang = {}
def execute_query(session):
    # create the transaction and execute query.
    return session.transaction().execute(
        'select 1 as cnt;',
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )
def execute(query, params):
#    with ydb.Driver(config) as driver:
    with drova as driver:
        try:
            driver.wait(timeout=5)
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            return None

        session = driver.table_client.session().create()
        prepared_query = session.prepare(query)

        return session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query,
            params,
            commit_tx=True
        )

def insert_state(id, state):
#    config = get_config()
    query = """
        DECLARE $id AS Utf8;
        DECLARE $state AS Utf8;
        UPSERT INTO user_state (user_id, state) VALUES ($id, $state);
        """
    params = {'$id': id, '$state': state}
    execute(query, params)
def find_state(id):
    print(id)
    query = """
        DECLARE $id AS Utf8;
        SELECT user_id,state FROM user_state WHERE user_id=$id;
        """
    params = {'$id': id}
    result_set = execute(query, params)
#    print(result_set)
    if not result_set or not result_set[0].rows:
        return None

    return result_set[0].rows[0].state

def handler(event, context):

    state1 = "user BYN"
    id = "50000077"
    insert_state(id, state1)
    id = "55523234"
    st = find_state(id)

    return {
        'statusCode': 200,
		'state': st
        
    }