
from download_wfs_configuration.dag_config import DAG_Configuration
from download_wfs_dags.task_base import TaskBase
from wtf_download_utils.utils import Utils


class CheckIfTrigger(TaskBase):
    def __init__(self, dag_config: DAG_Configuration = None):
        super().__init__(dag_config)
        self.utils = Utils(dag_config)
        
    def check_if_trigger(self):
        self.logger.info("Checking if the DAG should be triggered again.")
        query = """ select count(*) from public.state_execution_control where should_execute is TRUE; """
        result = self.dag_config.database.fetchone(query)
        should_trigger = result > 0
        if should_trigger:
            self.logger.info("There are UFs that still need to be processed. Triggering the DAG again.")
        else:
            self.logger.info("All UFs have been processed for the current cycle. No need to trigger the DAG again.")
        return should_trigger
    
    def prepare_task(self):
        self.dag_config.dag_config()
        return self.check_if_trigger()
        
def task_4_check_if_trigger(project_dir: str):
    import sys
    sys.path.append(project_dir)
    
    from download_wfs_dags.task_4_check_if_trigger import CheckIfTrigger
    from download_wfs_configuration.dag_config import DAG_Configuration

    dag_config = DAG_Configuration()
    task = CheckIfTrigger(dag_config)

    return task.prepare_task()
