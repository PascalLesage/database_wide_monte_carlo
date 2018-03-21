from brightway2 import *
import os, click

@click.command()
@click.option('--project_name', help='Name to give Brightway project', type=str)
@click.option('--ecospold_dirpath', help='Path to directory with ecoSPold2 files', type=str) 
@click.option('--database_name', type=str, help='Name to give imported database')
@click.option('--overwrite_database', type=bool, default=False, help='Overwrite database if it already exists')

def setup(project_name, ecospold_dirpath, database_name, overwrite_database):
    assert isinstance(project_name, str)
    assert isinstance(database_name, str)
    projects.set_current(project_name)
    print("Created project {}".format(project_name))
    bw2setup()
    should_import=True
    
    if database_name in databases:
        if not overwrite_database:
            print("Database {} already imported".format(database_name))
            should_import = False
        else:
            print("Database {} already existed, it will be deleted".format(database_name))
            Database(database_name).delete()
            Database(database_name).deregister()
            should_import=True
    if should_import:
        importer = SingleOutputEcospold2Importer(
            dirpath=ecospold_dirpath,
            db_name = database_name
        )
        importer.apply_strategies()
        importer.write_database()
    return None

if __name__ == "__main__":
    __spec__ = None
    setup()