
import json
from pathlib import Path
from airflow.hooks.base import BaseHook
from download_wfs_dags.output_database import OutputDatabase
from wtf_download_utils.utils import Utils
from airflow.models import Variable

class DAG_Configuration:
    
    def __init__(self):
        self.conf_path = Path(__file__).resolve().parent.parent / "download_wfs_configuration" / "dag-config.json"
        self.dag_config_json = self.conn_id = self.data_source = self.server = self.base_url = self.database = self.engine = self.utils = self.required_tables = self.output_dir = None
        self.files_to_analyze = []
        
    def dag_config(self):
        
        """Check if the dag-config.json file exists."""
        self.load_dag_config()
        
        """ Database Config """
        output_db = OutputDatabase()
        self.conn_id = self.check_simple_dag_config("output_database")
        self.database = output_db.get_database_facade(self.conn_id)
        self.engine = output_db.get_engine(self.conn_id)
        
        """ Base URL """
        self.base_url = self.check_simple_dag_config("base_url")
        
        """ Default folder for downloaded files """
        self.output_dir = Variable.get("DOWNLOAD_WFS_FOLDER")
        
        """ Utils """
        self.utils = Utils(dag_config=self)
        
        """ Required Tables """
        self.required_tables = self.check_simple_dag_config("required_tables")


    def load_dag_config(self):
        if not self.conf_path.exists():
            raise FileNotFoundError(f"dag-config.json file not found at {self.conf_path}")
        with open(self.conf_path) as json_data:
            self.dag_config_json = json.load(json_data)
        return self.dag_config_json
    
        
    def check_simple_dag_config(self, variable):
        if self.dag_config_json and self.dag_config_json.get(variable):
            value = self.dag_config_json.get(variable)  
            return value
        else:
            raise Exception(f"Missing {variable} config. (./download_wfs_configuration/dag-config.json)")