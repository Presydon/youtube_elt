def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234" 


def test_channel_handler(channel_handler):
    assert channel_handler == "MOCK_CHANNEL_HANDLER"


def test_postgres_conn(mock_postgrges_conn_vars):
    conn = mock_postgrges_conn_vars 
    assert conn.login == "mock_user"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db"


def test_dags_integrity(dagbag):
    #1. 
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("==================")
    print(dagbag.import_errors)


    #2.    
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("==================")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG '{dag_id}' not found in DagBag"

    #3 
    assert dagbag.size() == 3
    print(dagbag.size())

    #4 
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }
    print("==================")

    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        print(f"DAG '{dag_id}' has {actual_count} tasks (expected: {expected_count})")
        assert (
            expected_count == actual_count
        ), f"DAG '{dag_id}' has {actual_count} tasks, expected {expected_count}"
        print(dag_id, len(dag.tasks))