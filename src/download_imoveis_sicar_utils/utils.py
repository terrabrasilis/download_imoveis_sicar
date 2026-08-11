import pytz
from pathlib import Path
from datetime import datetime

class Utils:
    def __init__(self, dag_config):
        self.dag_config = dag_config
    
    def get_today_date(self):
        tz = pytz.timezone('America/Sao_Paulo')
        now = datetime.now(tz)
        return now.date()
    
    def unzip_shapefiles(self, folders: list):
        import zipfile
        import os

        for folder in folders:
            for file in os.listdir(folder):
                if file.endswith(".zip"):
                    
                    zip_path = os.path.join(folder, file)
                    zip_name = os.path.splitext(file)[0]
                    prefix = zip_name.replace("sicar_", "")

                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        for member in zip_ref.namelist():

                            filename = os.path.basename(member)
                            if not filename:
                                continue

                            name, ext = os.path.splitext(filename)
                            new_name = f"{name}_{prefix}{ext}"

                            source = zip_ref.open(member)
                            target_path = os.path.join(folder, new_name)

                            with open(target_path, "wb") as target:
                                target.write(source.read())

                    print(f"{file} descompactado")
                    
    def delete_files(self, folders: list, type):
        import os

        extensions = {
            "zip": [".zip"],
            "shapefile": [".shp", ".shx", ".dbf", ".prj", ".cst"]
        }

        for folder in folders:
            if not os.path.isdir(folder):
                print(f"Pasta não encontrada: {folder}")
                continue

            for file in os.listdir(folder):
                if any(file.endswith(ext) for ext in extensions.get(type, [])):
                    path = os.path.join(folder, file)

                    os.remove(path)
                    print(f"{file} deletado")