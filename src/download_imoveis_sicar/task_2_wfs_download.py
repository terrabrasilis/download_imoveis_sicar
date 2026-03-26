from time import time

from download_imoveis_sicar.task_base import TaskBase
from download_imoveis_sicar_configuration.dag_config import DAG_Configuration
from download_imoveis_sicar_utils.utils import Utils


class WFSDownload(TaskBase):
    def __init__(self, dag_config: DAG_Configuration = None):
        super().__init__(dag_config)
        self.utils = Utils(dag_config)
        self.qtd_features = 1000
        self.today = self.utils.get_today_date()
        self.current_year = self.today.year
        self.is_first_execution = True
        self.filters = []
        
    def is_first_cycle_run(self, logical_date):

        from datetime import datetime
        from airflow.models import DagRun # type: ignore

        logical_date = datetime.fromisoformat(logical_date)

        start_day = logical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = logical_date.replace(hour=23, minute=59, second=59)

        runs_today = DagRun.find(
            dag_id="download_imoveis_sicar",
            execution_start_date=start_day,
            execution_end_date=end_day
        )
        
        self.logger.info(f"Runs today: {len(runs_today)}")
        
        self.is_first_execution = len(runs_today) == 1
        return self.is_first_execution
        
    def get_uf_list(self):
        
        if  self.is_first_execution:
            self.logger.info("First execution of the day. Processing all UFs.")
            self.uf_list = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
            return self.uf_list
        
        query = """ SELECT state_code FROM public.state_execution_control where should_execute = true; """
        result = self.dag_config.database.fetchall(query)
        print(f"UFs to process: {[row[0] for row in result]}")
        self.uf_list = [row[0] for row in result]
        return self.uf_list
    
    def disable_uf_execution(self, uf):
        if self.year >= self.current_year:
            self.logger.info(f"UF {uf} has already been processed for the current year. Disabling execution.")
            try:
                query = f"""
                    UPDATE public.state_execution_control
                    SET should_execute = false
                    WHERE state_code = '{uf}';
                """
                self.dag_config.database.execute(query)
                self.dag_config.database.commit()
            except Exception as e:
                self.logger.error(f"Error disabling execution for UF {uf}: {e}")
                
    def validate_and_cleanup_shapefile(self, uf, directory_path, total_records):

        if self.current_year != self.year:
            return True

        query = f"""
            SELECT shapefile_count 
            FROM public.state_execution_control 
            WHERE state_code = '{uf}'
        """

        result = self.dag_config.database.fetchone(query)
        if not result:
            return False
        
        if result != total_records:

            delete_query = f"""
                DELETE FROM public.sicar_shapefile_downloads
                WHERE state_code = '{uf}'
                AND year = '{self.year}'
                AND directory_path = '{directory_path}'
            """

            self.dag_config.database.execute(delete_query)
            self.dag_config.database.commit()

            self.utils.delete_files([directory_path], "zip")

            self.logger.info(f"Deleted record and file for UF {uf} due to mismatch in total records.")
            return False
        else:
            return True
            
    
    def get_period_by_uf(self, uf):
        query = f"""
            SELECT last_executed_year + 1 AS next_year
            FROM public.state_execution_control
            WHERE state_code = '{uf}';
        """
        result = self.dag_config.database.fetchone(query)

        if result is None:
            raise Exception(f"No year found for UF {uf}")

        self.year = result

        if self.year is None:
            raise Exception(f"Invalid year calculated for UF {uf}")
        
        if self.year > self.current_year:
            self.year = self.current_year

        return self.year
    
    def update_state_execution_control(self, uf, year, total_records):
        self.logger.info(f"Updating state_execution_control for UF {uf} and year {year} with total records {total_records}")
        try:
            query = f"""
                UPDATE public.state_execution_control
                SET last_executed_year = {year}, shapefile_count = {total_records}
                WHERE state_code = '{uf}';
            """
            self.dag_config.database.execute(query)
            self.dag_config.database.commit()
        except Exception as e:
            self.logger.error(f"Error updating state_execution_control for UF {uf} and year {year}: {e}")

    def get_total_records(self, session, base_url, type_name, filters):

        import xml.etree.ElementTree as ET
        import time
        import requests

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": type_name,
            "CQL_FILTER": filters,
            "srsName": "urn:ogc:def:crs:EPSG::4326"
        }

        for attempt in range(5):
            try:
                response = session.get(base_url, params=params, timeout=180)
                break
            except requests.exceptions.ReadTimeout:
                self.logger.warning(
                    f"Timeout getting total records for {type_name}. Retry {attempt+1}/5"
                )
                time.sleep(10)
        else:
            self.logger.error(f"Failed to get total records for {type_name}")
            return 0

        if response.status_code != 200:
            self.logger.error(f"Error fetching total for {type_name}: {response.text}")
            return 0

        try:
            root = ET.fromstring(response.content)
            total = int(root.attrib.get('numberMatched', 0))
        except Exception as e:
            self.logger.error(f"Error parsing XML for {type_name}: {e}")
            return 0

        return total
    
    def verify_file_exists(self, folder_path, file_name):
        query = f"""
            SELECT 1
            FROM public.sicar_shapefile_downloads
            WHERE directory_path = '{folder_path}' AND file_name = '{file_name}' LIMIT 1;
        """
        result = self.dag_config.database.fetchone(query)
        return result > 0 if result else False
    
    def inset_download_record(self, uf, year, folder_path, file_name):
        try:
            query = f"""
                INSERT INTO public.sicar_shapefile_downloads(
                state_code, year, directory_path, file_name, imported)
                VALUES ('{uf}', '{year}', '{folder_path}', '{file_name}', 'false');
            """
            self.dag_config.database.execute(query)
        except Exception as e:
            self.logger.error(f"Error inserting download record for {uf} {year}: {e}")
            return False
    
    def get(self):

        import os, math, requests, time
        
        session = requests.Session()
                
        for uf in self.uf_list:
            
            self.get_period_by_uf(uf)

            self.filters = []
            if self.year == self.current_year:
                self.filters = [f"dat_criacao >= '{self.year}-01-01T00:00:00' AND dat_criacao <= '{self.year}-12-31T23:59:59'", f"data_atualizacao >= '{self.today}T00:00:00' AND data_atualizacao <= '{self.today}T23:59:59'"]
            else:
                self.filters = [f"dat_criacao >= '{self.year}-01-01T00:00:00' AND dat_criacao <= '{self.year}-12-31T23:59:59'"]

            for filter in self.filters:
            
                base_url = self.dag_config.base_url
                type_name = f'sicar:sicar_imoveis_{uf.lower()}'
                
                self.logger.info(f"Fetching data for UF: {uf}, Year: {self.year}, Filters: {filter}")

                total_records = self.get_total_records(session, base_url, type_name, filter)
                
                if total_records == 0:
                    self.logger.info(f"No records found for {uf}")
                    self.update_state_execution_control(uf, self.year, total_records)
                    continue

                self.logger.info(f"Total records found: {total_records}")

                total_pages = math.ceil(total_records / self.qtd_features)

                for pagina in range(total_pages):

                    start_index = pagina * self.qtd_features

                    params = {
                        'service': 'WFS',
                        'version': '2.0.0',
                        'request': 'GetFeature',
                        'typeNames': type_name,
                        'count': self.qtd_features,
                        'startIndex': start_index,
                        'outputFormat': 'SHAPE-ZIP',
                        'CQL_FILTER': filter
                    }                    
                    
                    for attempt in range(5):
                        try:
                            response = session.get(base_url, params=params, timeout=180)
                            break
                        except requests.exceptions.ReadTimeout:
                            self.logger.info(f"Timeout for {uf} page {pagina}. Retry {attempt+1}/5")
                            time.sleep(10)
                    else:
                        self.logger.error(f"Failed to download page {pagina} for {uf}")
                        continue

                    if response.status_code != 200:
                        self.logger.error(f"Error fetching data for {type_name}: {response.text}")
                        break
                    
                    folder_path = f"{self.dag_config.output_dir}/{uf}_{self.year}"
                    os.makedirs(folder_path, exist_ok=True)

                    file_name = f"sicar_{uf}_{self.year}_{start_index}.zip"
                    full_file_name = f"{folder_path}/sicar_{uf}_{self.year}_{start_index}.zip"
                    
                    download_file = True
                    file_exists = self.verify_file_exists(folder_path, file_name)
                    
                    if file_exists:
                        self.logger.info(f"File already exists in database: {folder_path}")
                        is_valid_shapefile = self.validate_and_cleanup_shapefile(
                            uf,
                            folder_path,
                            total_records
                        )
                        download_file = not is_valid_shapefile
                        
                    if download_file:
                        with open(full_file_name, "wb") as f:
                            f.write(response.content)
                        self.inset_download_record(uf, self.year, folder_path, file_name)
                        self.logger.info(f"Saved: {full_file_name}")
                        
                self.update_state_execution_control(uf, self.year, total_records)
                self.logger.info(f"Total number of pages downloaded for {uf}: {total_pages}")
                
            self.dag_config.database.commit()
            self.disable_uf_execution(uf)
    
    def prepare_task(self, logical_date):
        self.dag_config.dag_config()
        self.is_first_cycle_run(logical_date=logical_date)
        self.get_uf_list()
        self.get()
        return True

def task_2_wfs_download(project_dir: str, logical_date):
    import sys
    sys.path.append(project_dir)
    
    from download_imoveis_sicar.task_2_wfs_download import WFSDownload
    from download_imoveis_sicar_configuration.dag_config import DAG_Configuration

    dag_config = DAG_Configuration()
    task = WFSDownload(dag_config)

    return task.prepare_task(logical_date)
