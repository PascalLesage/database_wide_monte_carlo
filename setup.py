from brightway2 import *
from brightway2 import *
import os, click

@click.command()
@click.option('--project_name', help='Name to give Brightway project', type=str)
@click.option('--ecospold_dirpath', help='Path to directory with ecoSPold2 files', type=str) 
@click.option('--database_name', type=str, help='Name to give imported database')

def main(project_name, ecospold_dirpath, database_name):
	assert isinstance(project_name, str)
	assert isinstance(database_name, str)
	
	projects.create_project(project_name)
	projects.set_current(project_name)
	bw2setup()
	importer = SingleOutputEcospold2Importer(
		dirpath=ecospold_dirpath,
		db_name = database_name
	)
	importer.apply_strategies()
	importer.write_database()
	return None

if __name__ == "__main__":
    main()