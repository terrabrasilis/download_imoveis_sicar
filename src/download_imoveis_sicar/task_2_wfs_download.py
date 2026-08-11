import os
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
        self.last_executed_year = None
        self.first_year = 2013
        self.retroactive_years_to_check = 0
        self.retroactive_tolerance = 5
        
    def is_first_cycle_run(self, dag_run_conf):

        self.is_first_execution = not dag_run_conf or dag_run_conf in ("None", "", "{}")
        self.logger.info(f"is_first_execution: {self.is_first_execution}")
        return self.is_first_execution
        
    def get_uf_list(self):
        
        if  self.is_first_execution:
            self.logger.info("First execution of the day. Re-enabling all UFs.")
            query = "UPDATE public.state_execution_control SET should_execute = true;"
            self.dag_config.database.execute(query)
            self.dag_config.database.commit()
        
        query = """ SELECT state_code FROM public.state_execution_control where should_execute = true ORDER BY state_code LIMIT 1; """
        result = self.dag_config.database.fetchall(query)
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
                
    def validate_and_cleanup_shapefile(self, uf, directory_path, total_records, filter_type, file_name):
        
        if str(self.current_year) != str(self.year):
            return True

        if filter_type != "insert":
            return True

        query = f"""
            SELECT shapefile_count
            FROM public.state_execution_control 
            WHERE state_code = '{uf}'
        """

        result = self.dag_config.database.fetchone(query)
        if result is None:
            return False
        
        if str(result) != str(total_records):
            delete_query = f"""
                DELETE FROM public.sicar_shapefile_downloads
                WHERE state_code = '{uf}'
                AND year = '{self.year}'
                AND directory_path = '{directory_path}'
                AND file_name = '{file_name}'
            """

            self.dag_config.database.execute(delete_query)
            self.dag_config.database.commit()

            full_file_name = f"{directory_path}/{file_name}"
            if os.path.exists(full_file_name):
                os.remove(full_file_name)

            self.logger.info(f"Deleted record and file for UF {uf} due to mismatch in total records.")
            return False
        else:
            return True
            
    
    def get_period_by_uf(self, uf):
        query = f"""
            SELECT last_executed_year
            FROM public.state_execution_control
            WHERE state_code = '{uf}';
        """
        result = self.dag_config.database.fetchone(query)

        if result is None:
            raise Exception(f"No year found for UF {uf}")

        self.last_executed_year = int(result)
        self.year = self.last_executed_year + 1

        if self.year > self.current_year:
            self.year = self.current_year

        return self.year

    def get_forward_years(self):
        if self.last_executed_year is None:
            return [self.current_year]

        next_year = self.last_executed_year + 1
        if next_year > self.current_year:
            return [self.current_year]

        return list(range(next_year, self.current_year + 1))
    
    def update_state_execution_control(self, uf, year, total_records, filter_type):
        self.logger.info(f"Updating state_execution_control for UF {uf} and year {year} with total records {total_records}")
        try:
            
            fild_to_update = "shapefile_count" if filter_type == "insert" else "updated_shapefile_count"
            query = f"""
                UPDATE public.state_execution_control
                SET last_executed_year = GREATEST(COALESCE(last_executed_year, {year}), {year}),
                    {fild_to_update} = {total_records}
                WHERE state_code = '{uf}';
            """
            self.dag_config.database.execute(query)
            self.dag_config.database.commit()
        except Exception as e:
            self.logger.error(f"Error updating state_execution_control for UF {uf} and year {year}: {e}")

    def get_total_records(self, session, base_url, type_name, filters=None):

        import xml.etree.ElementTree as ET
        import time
        import requests

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": type_name,
            "resultType": "hits"
        }

        if filters:
            params["CQL_FILTER"] = filters

        for attempt in range(5):
            try:
                response = session.get(base_url, params=params, timeout=180)
                break
            except requests.exceptions.ReadTimeout:
                self.logger.info(
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
    
    def build_filters(self, year):

        filters = [
            {
                "type": "insert",
                "query": f"dat_criacao >= '{year}-01-01T00:00:00' AND dat_criacao <= '{year}-12-31T23:59:59'"
            }
        ]

        if year == self.current_year:
            filters.append(
                {
                    "type": "updated",
                    "query": f"data_atualizacao >= '{self.today}T00:00:00' AND data_atualizacao <= '{self.today}T23:59:59'"
                }
            )

        return filters

    def get_local_total(self, uf):
        query = f"""
            SELECT count(*)
            FROM public.sicar_geometries
            WHERE uf = '{uf}';
        """
        result = self.dag_config.database.fetchone(query)
        return int(result) if result is not None else 0

    def get_stored_count(self, uf, year):
        query = f"""
            SELECT shapefile_count
            FROM public.sicar_year_control
            WHERE state_code = '{uf}' AND year = {year};
        """
        return self.dag_config.database.fetchone(query)

    def upsert_year_control(self, uf, year, count):
        query = f"""
            INSERT INTO public.sicar_year_control(state_code, year, shapefile_count)
            VALUES ('{uf}', {year}, {count})
            ON CONFLICT (state_code, year)
            DO UPDATE SET shapefile_count = EXCLUDED.shapefile_count,
                          updated_at = now();
        """
        self.dag_config.database.execute(query)

    def get_retroactive_check_years(self):

        if self.last_executed_year is None:
            return []

        last_check = min(self.last_executed_year, self.current_year - 1)

        if last_check < self.first_year:
            return []

        if self.retroactive_years_to_check and self.retroactive_years_to_check > 0:
            first_check = max(self.first_year, last_check - self.retroactive_years_to_check + 1)
        else:
            first_check = self.first_year

        return list(range(first_check, last_check + 1))

    def check_retroactive_years(self, uf, session):

        years_to_resync = []
        base_url = self.dag_config.base_url
        type_name = f'sicar:sicar_imoveis_{uf.lower()}'

        for year in self.get_retroactive_check_years():

            filter_query = self.build_filters(year)[0]["query"]
            live_count = self.get_total_records(session, base_url, type_name, filter_query)
            stored_count = self.get_stored_count(uf, year)

            if stored_count is None:
                if live_count == 0:
                    self.upsert_year_control(uf, year, live_count)
                    self.logger.info(
                        f"No baseline for UF {uf}, year {year}. No records found, baseline seeded with 0."
                    )
                else:
                    self.logger.info(
                        f"No baseline for UF {uf}, year {year}. Force re-download because there is no previous sync record."
                    )
                    years_to_resync.append(year)
                continue

            if abs(live_count - stored_count) > self.retroactive_tolerance:
                self.logger.info(
                    f"Retroactive data detected for UF {uf}, year {year}: "
                    f"SICAR has {live_count}, last synced count {stored_count}. Re-downloading."
                )
                years_to_resync.append(year)
            elif live_count != stored_count:
                self.upsert_year_control(uf, year, live_count)
                self.logger.info(
                    f"Count drift within tolerance for UF {uf}, year {year} "
                    f"({stored_count} -> {live_count}). Baseline updated without re-download."
                )

        return years_to_resync

    def check_global_count(self, uf, session):

        base_url = self.dag_config.base_url
        type_name = f'sicar:sicar_imoveis_{uf.lower()}'

        live_total = self.get_total_records(session, base_url, type_name)
        local_total = self.get_local_total(uf)

        if abs(live_total - local_total) > self.retroactive_tolerance:
            self.logger.info(
                f"Total count mismatch for UF {uf}: SICAR has {live_total}, local database has {local_total}. "
                f"Some records may be missing. Force a re-download of the affected years by deleting their "
                f"sicar_year_control rows."
            )
        else:
            self.logger.info(f"Total count OK for UF {uf}: {live_total} records.")

    def reset_year_folder(self, uf, year):

        folder_path = f"{self.dag_config.output_dir}/{uf}_{year}"
        query = f"""
            DELETE FROM public.sicar_shapefile_downloads
            WHERE state_code = '{uf}' AND year = '{year}';
        """
        self.dag_config.database.execute(query)
        self.dag_config.database.commit()
        self.utils.delete_files([folder_path], "zip")
        self.logger.info(f"Reset download records and files for UF {uf}, year {year}.")

    def process_year(self, session, uf, year, force_download=False, update_control=True):

        import math, os, requests, time

        self.year = year
        base_url = self.dag_config.base_url
        type_name = f'sicar:sicar_imoveis_{uf.lower()}'

        for filter in self.build_filters(year):

            filter_type = filter["type"]
            filter_query = filter["query"]

            self.logger.info(f"Fetching data for UF: {uf}, Year: {year}, Type: {filter_type}, Filters: {filter_query}")

            total_records = self.get_total_records(session, base_url, type_name, filter_query)

            if total_records == 0:
                self.logger.info(f"No records found for {uf} year {year}")
                if filter_type == "insert":
                    self.upsert_year_control(uf, year, 0)
                if update_control:
                    self.update_state_execution_control(uf, year, total_records, filter_type)
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
                    'CQL_FILTER': filter_query
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

                folder_path = f"{self.dag_config.output_dir}/{uf}_{year}"
                os.makedirs(folder_path, exist_ok=True)

                filter_suffix = "" if filter_type == "insert" else f"_{filter_type}_{self.today}"

                file_name = f"sicar_{uf}_{year}{filter_suffix}_{start_index}.zip"
                full_file_name = f"{folder_path}/{file_name}"

                download_file = True
                file_exists = self.verify_file_exists(folder_path, file_name)

                if file_exists and not force_download:
                    self.logger.info(f"File already exists in database: {folder_path}")
                    is_valid_shapefile = self.validate_and_cleanup_shapefile(
                        uf,
                        folder_path,
                        total_records,
                        filter_type,
                        file_name
                    )
                    download_file = not is_valid_shapefile

                if download_file:
                    with open(full_file_name, "wb") as f:
                        f.write(response.content)
                    self.inset_download_record(uf, year, folder_path, file_name)
                    self.logger.info(f"Saved: {full_file_name}")

            if filter_type == "insert":
                self.upsert_year_control(uf, year, total_records)
            if update_control:
                self.update_state_execution_control(uf, year, total_records, filter_type)
            self.logger.info(f"Total number of pages downloaded for {uf}: {total_pages}")

    def get(self):

        import requests

        session = requests.Session()

        self.retroactive_years_to_check = self.dag_config.retroactive_years_to_check
        self.retroactive_tolerance = self.dag_config.retroactive_tolerance

        for uf in self.uf_list:

            self.get_period_by_uf(uf)
            forward_years = self.get_forward_years()

            retroactive_years = (
                self.check_retroactive_years(uf, session)
                if self.dag_config.check_retroactive_data
                else []
            )
            self.check_global_count(uf, session)

            for year in retroactive_years:
                self.reset_year_folder(uf, year)
                self.process_year(session, uf, year, force_download=True, update_control=False)

            if forward_years:
                for year in forward_years:
                    self.process_year(session, uf, year)

            self.dag_config.database.commit()
            self.disable_uf_execution(uf)
    
    def prepare_task(self, dag_run_conf=None):
        self.dag_config.dag_config()
        self.is_first_cycle_run(dag_run_conf=dag_run_conf)
        self.get_uf_list()
        self.get()
        return True

def task_2_wfs_download(project_dir: str, dag_run_conf=None):
    import sys
    sys.path.append(project_dir)
    
    from download_imoveis_sicar.task_2_wfs_download import WFSDownload
    from download_imoveis_sicar_configuration.dag_config import DAG_Configuration

    dag_config = DAG_Configuration()
    task = WFSDownload(dag_config)

    return task.prepare_task(dag_run_conf)
