from sqlalchemy import create_engine

from airflow.hooks.base import BaseHook

from wtf_download_utils.database_facade import DatabaseFacade


class OutputDatabase:

    def __init__(self):
        self.database: DatabaseFacade = None

    def __get_airflow_conn_config(self, conn_id):
        """Return a json with airflow connection."""

        conn_config = BaseHook.get_connection(conn_id)

        if conn_config:
            conn_config.set_extra("")
            return conn_config
        else:
            return None

    def get_database_facade(self, connection_key, keep_connection=False) -> "DatabaseFacade":
        """Return a DatabaseFacade of the requested connection key."""

        print("Retrieving database connection: " + connection_key)

        conn_config = self.__get_airflow_conn_config(connection_key)

        if not conn_config == None:

            database = DatabaseFacade.from_url(conn_config.get_uri())

            if (database):

                if (keep_connection == True):

                    self.databases[connection_key] = database

                return database
            else:
                raise Exception("Unable to connect to database. Connection Key: " +
                                connection_key + " and Connection Id: "+conn_config.conn_id+" )")

        else:
            raise Exception(
                "Connection config not found on Airflow connections. Connection key: "+connection_key+")")
        
    def get_engine(self, connection_key):
        """Return a SQLAlchemy engine for the requested connection key."""
        conn_config = self.__get_airflow_conn_config(connection_key)
        if conn_config:
            uri = conn_config.get_uri().replace("postgres://", "postgresql://", 1)
            return create_engine(uri)
        else:
            raise Exception("Connection config not found on Airflow connections. Connection key: "+connection_key+")")