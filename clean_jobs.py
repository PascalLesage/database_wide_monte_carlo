
# coding: utf-8

import os
import shutil
import glob
import click
from collections import defaultdict
import json
import datetime

@click.command()
@click.option('--base_dir', help='Root directory for all presampling files', type=str)
@click.option('--database_name', help='Name of database', type=str)
@click.option('--database_size', help='Number of activities in database', type=int)
@click.option('--include_inventory', default=True, type=bool)
@click.option('--include_matrices', default=False, type=bool)
@click.option('--include_supply', default=False, type=bool)

def clean_jobs(base_dir,
               database_name,
               database_size, 
               include_inventory=True, 
               include_matrices=False, 
               include_supply=False
               ):
    """Delete jobs or iterations within jobs that have missing files"""

    if not any([include_inventory, include_supply, include_matrices]):
        print("No output requested. At least one of the following must be true:")
        print("include_inventory, include_supply or include_matrices")
        sys.exit(0)

    job_dir = os.path.join(base_dir, database_name, 'jobs')
    jobs = glob.glob(job_dir+'/*/')
    print("Cleaning up jobs: {}".format(jobs))
    jobs_to_delete = []
    iterations_to_delete = defaultdict(list)
    
    for job in jobs:
        job_folders = glob.glob(os.path.join(job_dir, job)+'/*/')
        
        for job_folder in job_folders:
            if "common_files" in job_folder:
                if len(os.listdir(job_folder)) != 11:
                    print("job to be deleted: {}, because it had {} files".format(
                        job, len(os.listdir(job_folder)))
                        )
                    jobs_to_delete.append(job)
            else:
                # Check if inventory samples are there, if required
                if include_inventory:
                    try:
                        if len(os.listdir(os.path.join(job_folder, 'Inventory')))!=database_size:
                            iterations_to_delete[job_folder].append('missing some inventory results')
                    except OSError:
                        iterations_to_delete[job_folder].append('no inventory')
                
                # Check if supply vector samples are there, if required
                if include_supply:
                    try:
                        if len(os.listdir(os.path.join(job_folder, 'Supply')))!=database_size:
                            iterations_to_delete[job_folder].append('missing some supply array results')
                    except OSError:
                        iterations_to_delete[job_folder].append('no supply arrays')

                # Check if matrices present, if required
                if include_matrices:
                    try:
                        if 'A_matrix.npy' not in os.listdir(os.path.join(job_folder, 'Matrices')):
                            iterations_to_delete[job_folder].append('no A matrix')
                    except OSError:
                        iterations_to_delete[job_folder].append('no A matrix')

                    try:
                        if 'B_matrix.npy' not in os.listdir(os.path.join(job_folder, 'Matrices')):
                            iterations_to_delete[job_folder].append('no B matrix')
                    except OSError:
                        iterations_to_delete[job_folder].append('no B matrix')

    if len(jobs_to_delete)>0:
        print("Will delete following jobs: {}".format(jobs_to_delete))
    else:
        print("No jobs to delete")
    
    if len(iterations_to_delete)>0:
        print("Will delete the following iterations: ")
        for iteration, reason in iterations_to_delete.items():
            print("\t{}:{}".format(iteration, reason))
    else:
        print("No iterations to delete")
    
    if len(jobs_to_delete) + len(iterations_to_delete) > 0:
        understood = False
        while not understood:
            c = input("Delete jobs/iterations? (y/n)")
            if c == "n":
                understood = True
                exit()
            elif c == "y":
                understood = True
                for job in jobs_to_delete:
                    shutil.rmtree(job, ignore_errors=True)
                for iteration in iterations_to_delete.keys():
                    shutil.rmtree(iteration, ignore_errors=True)
            else:
                pass
    log = {'cleaned':
            {
                'Matrices':include_matrices*1,
                'Inventory': include_inventory*1,
                'Supply': include_supply*1
            }
          }
    for job in jobs:
        try:
            with open(os.path.join(job, 'log.json'), 'w') as f:
                log = json.load(f)
                print("couldn't open log")
        except:
            log = {}
        now = datetime.datetime.now()
        log['cleaned'] = {
                    'included_elements': 
                        {
                            'Matrices':include_matrices*1,
                            'Inventory': include_inventory*1,
                            'Supply': include_supply*1
                        },
                    'completed': 
                        "{}-{}-{}_{}h{}".format(
                            now.year,
                            now.month,
                            now.day,
                            now.hour,
                            now.minute)
            }
        with open(os.path.join(job, 'log.json'), 'w') as f:
            json.dump(log, f, indent=4)

if __name__ == '__main__':
    clean_jobs()
