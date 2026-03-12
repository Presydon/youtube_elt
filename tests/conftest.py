import os 
import pytest 
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag

@pytest.fixture
def api_key():
    with mock.patch.dict(os.environ, AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")
        

@pytest.fixture
def channel_handler(): 
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLER="MOCK_CHANNEL_HANDLER"):
        yield Variable.get("CHANNEL_HANDLER")


@pytest.fixture
def mock_postgrges_conn_vars(): 
    conn = Connection(
        login="mock_user",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db"
    ) 
    conn_uri = conn.get_uri()

    with mock.patch.dict(os.environ, AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")


@pytest.fixture
def dagbag():
    yield DagBag()


@pytest.fixture
def airflow_variable():
    def get_airflow_variables(variable_name): 
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"     
        return os.getenv(env_var)
    return get_airflow_variables


@pytest.fixture
def real_postgres_connection(): 
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None 

    try: 
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )

        yield conn

    except psycopg2.Error as e:
        pytest.fail(f"failed to connect to the database: {e}")

    finally:
        if conn:
            conn.close()