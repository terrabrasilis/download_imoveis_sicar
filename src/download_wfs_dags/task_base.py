from wtf_download_utils.logger import TasksLogger
from download_wfs_configuration.dag_config import DAG_Configuration

class TaskBase:

        logger = None
        dag_config: DAG_Configuration = None

        def __init__(self, dag_config: DAG_Configuration = None):
                self.dag_config = dag_config
                self.logger = TasksLogger("TASK LOGGING")
                self.logger.setLoggerLevel("DEBUG")
