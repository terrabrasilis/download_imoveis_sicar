
from download_wfs_configuration.dag_config import DAG_Configuration
from download_wfs_dags.task_base import TaskBase
from wtf_download_utils.utils import Utils


class InsertShapeFile(TaskBase):
    def __init__(self, dag_config: DAG_Configuration = None):
        super().__init__(dag_config)
        self.utils = Utils(dag_config)
        self.folders_to_process = []
        
    def get_shapefile_folder(self):   
        query = """SELECT DISTINCT directory_path from public.sicar_shapefile_downloads where imported = false;"""
        result = self.dag_config.database.fetchall(query)
        
        if result is None or len(result) == 0:
            self.logger.info("No shapefile folders to process.")
            return
        
        for row in result:
            self.folders_to_process.append(row[0])
        self.logger.info(f"All shapefile folders to process were found.") 
        
    def update_imported_status(self, folder):
        query = f"""UPDATE public.sicar_shapefile_downloads SET imported = true WHERE directory_path = '{folder}';"""
        self.dag_config.database.execute(query)
        self.dag_config.database.commit()
        self.logger.info(f"Updated imported status for folder: {folder}")  

    def insert_shapefile(self):
        import pandas as pd  # type: ignore
        import geopandas as gpd  # type: ignore
        import os
        import tempfile
        from sqlalchemy import text # type: ignore
        from shapely.geometry import MultiPolygon # type: ignore

        engine = self.dag_config.engine        

        try:
            for folder in self.folders_to_process:
                for file in os.listdir(folder):

                    if file.endswith(".shp"):
                        path = os.path.join(folder, file)

                        self.logger.info(f"Processing {path}")

                        gdf = gpd.read_file(path)

                        # remover geometrias nulas
                        gdf = gdf[gdf.geometry.notnull()]

                        # remover duplicados dentro do shapefile
                        gdf = gdf.drop_duplicates(subset="cod_imovel")

                        # corrigir Polygon -> MultiPolygon
                        gdf["geometry"] = gdf["geometry"].apply(
                            lambda geom: MultiPolygon([geom]) if geom.geom_type == "Polygon" else geom
                        )
                        
                        print(gdf.columns)

                        # converter geometria para WKT
                        gdf["geometry"] = gdf.geometry.apply(lambda g: g.wkt if g else None)

                        # garantir ordem das colunas
                        columns = [
                            "cod_imovel",
                            "status_imo",
                            "dat_criaca",
                            "data_atual",
                            "area",
                            "condicao",
                            "uf",
                            "municipio",
                            "cod_munici",
                            "m_fiscal",
                            "tipo_imove",
                            "geometry"
                        ]
                        
                        for col in columns:
                            if col not in gdf.columns:
                                gdf[col] = None

                        # converter datas
                        gdf["dat_criaca"] = pd.to_datetime(gdf["dat_criaca"], errors="coerce")
                        gdf["data_atualizacao"] = pd.to_datetime(gdf["data_atual"], errors="coerce")
                        
                        gdf = gdf[columns]

                        # criar csv temporário
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")

                        gdf.to_csv(
                            tmp.name,
                            index=False,
                            sep=",",
                            na_rep=""
                        )

                        self.logger.info(f"Temporary CSV created: {tmp.name}")

                        with engine.begin() as conn:

                            # recriar tabela temporária
                            conn.execute(text("""
                                DROP TABLE IF EXISTS tmp_sicar_geometries;

                                CREATE TEMP TABLE tmp_sicar_geometries
                                (LIKE sicar_geometries INCLUDING ALL);
                            """))

                            # COPY para postgres
                            raw = conn.connection
                            with open(tmp.name, "r") as f:
                                cursor = raw.cursor()
                                cursor.copy_expert(
                                    "COPY tmp_sicar_geometries FROM STDIN WITH CSV HEADER",
                                    f
                                )

                            # salvar duplicados (antes de inserir novos)
                            conn.execute(text("""
                                INSERT INTO sicar_geometries_duplicates
                                SELECT t.*
                                FROM tmp_sicar_geometries t
                                WHERE EXISTS (
                                    SELECT 1
                                    FROM sicar_geometries g
                                    WHERE g.cod_imovel = t.cod_imovel
                                )
                            """))

                            # inserir novos registros
                            conn.execute(text("""
                                INSERT INTO sicar_geometries
                                SELECT t.*
                                FROM tmp_sicar_geometries t
                                WHERE NOT EXISTS (
                                    SELECT 1
                                    FROM sicar_geometries g
                                    WHERE g.cod_imovel = t.cod_imovel
                                )
                            """))

                        self.update_imported_status(folder)

                        self.logger.info(
                            f"Shapefile processed successfully ({len(gdf)} registros processados)"
                        )

        except Exception as e:
            self.logger.error(f"Error inserting shapefile: {str(e)}")
            raise
        
    def update_geometries(self):
        query = """
            UPDATE public.sicar_geometries g
            SET
                (status_imo, dat_criaca, data_atual, area, condicao, uf, municipio, cod_munici, m_fiscal, tipo_imove, geometry) =
                (d.status_imo, d.dat_criaca, d.data_atual, d.area, d.condicao, d.uf, d.municipio, d.cod_munici, d.m_fiscal, d.tipo_imove, d.geometry)
            FROM public.sicar_geometries_duplicates d
            WHERE g.cod_imovel = d.cod_imovel;

            DELETE FROM public.sicar_geometries_duplicates d
            USING public.sicar_geometries g
            WHERE g.cod_imovel = d.cod_imovel;
        """
        
        self.dag_config.database.execute(query)
        self.dag_config.database.commit()
        self.logger.info("Geometries updated and duplicates removed successfully.")
    
    def prepare_task(self):
        try:
            self.dag_config.dag_config()
            self.get_shapefile_folder()
            self.utils.unzip_shapefiles(self.folders_to_process)
            self.insert_shapefile()
            self.update_geometries()
            self.utils.delete_files(self.folders_to_process, "shapefile")
            return True
        except Exception as e:
            self.logger.error(f"Task failed: {str(e)}")
            raise
        
def task_3_insert_shape_file(project_dir: str):
    import sys
    sys.path.append(project_dir)
    
    from download_wfs_dags.task_3_insert_shapefile import InsertShapeFile
    from download_wfs_configuration.dag_config import DAG_Configuration

    dag_config = DAG_Configuration()
    task = InsertShapeFile(dag_config)

    return task.prepare_task()
