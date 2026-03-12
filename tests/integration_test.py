import requests
import pytest
import psycopg2

def test_youtube_api_response(airflow_variable):
    API_KEY = airflow_variable("API_KEY")
    CHANNEL_HANDLE = airflow_variable("CHANNEL_HANDLER")

    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

    try: 
        response = requests.get(url)
        assert response.status_code == 200
    except requests.exceptions.RequestException as e:
        pytest.fail(f"API request failed: {e}")


def test_real_postgres_connection(real_postgres_connection): 
    cursor = None

    try: 
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
        pytest.fa(f"Database query faailure: {e}")

    finally: 
        if cursor is not None:
            cursor.close()