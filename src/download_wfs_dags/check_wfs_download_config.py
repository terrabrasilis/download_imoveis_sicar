from download_wfs_dags.task_base import TaskBase
from download_wfs_configuration.dag_config import DAG_Configuration


class CheackDownloadWFSConfig(TaskBase):
    def __init__(self, dag_config: DAG_Configuration = None):
        super().__init__(dag_config)

    def verify_table_exists(self):
        tables = self.dag_config.required_tables
        
        for table in tables:
            if not self.dag_config.database.table_exist(table, "public"):
                self.logger.error(f"Table {table} does not exist.")
                return False
        self.logger.info("All required tables exist.")
        return True

    def prepare_task(self):
        self.logger.info("Checking DAG Config")
        self.dag_config.dag_config()

        checks = [
            (self.verify_table_exists, "Required tables are missing.")
        ]

        for check, error_msg in checks:
            if not check():
                raise Exception(error_msg)

        self.logger.info("Configuration check passed.")
        return True


def check_wfs_download_config(project_dir: str):
    import sys
    sys.path.append(project_dir)
    
    from download_wfs_dags.check_wfs_download_config import CheackDownloadWFSConfig
    from download_wfs_configuration.dag_config import DAG_Configuration

    dag_config = DAG_Configuration()
    task = CheackDownloadWFSConfig(dag_config)

    return task.prepare_task()
