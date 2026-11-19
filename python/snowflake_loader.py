
import os

import snowflake.connector
from dotenv import load_dotenv


# Load environment variables from project root
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

ENV_FILE = os.path.join(PROJECT_ROOT, ".env")

load_dotenv(ENV_FILE)


def get_snowflake_connection():
    """
    Establish a connection to Snowflake.
    """

    required_variables = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD"
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise ValueError(
            f"Missing environment variables: {missing_variables}"
        )

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    return connection


if __name__ == "__main__":

    connection = None

    try:
        connection = get_snowflake_connection()

        cursor = connection.cursor()

        cursor.execute("""
            SELECT
                CURRENT_ACCOUNT(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA()
        """)

        result = cursor.fetchone()

        print("\nSnowflake connection successful!")
        print("Account:", result[0])
        print("Role:", result[1])
        print("Warehouse:", result[2])
        print("Database:", result[3])
        print("Schema:", result[4])

        cursor.close()

    except Exception as error:
        print("Snowflake connection failed:", error)
        raise

    finally:
        if connection:
            connection.close()
