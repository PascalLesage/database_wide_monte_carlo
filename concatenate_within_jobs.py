import os
import shutil
import numpy as np
import pickle
import time
import multiprocessing as mp
from math import ceil
import click
import glob
import json
import datetime

""" Concatenate samples within jobs and store in a temp. directory.
    Jobs should previously have been cleaned using `clean_jobs.py`
    Arrays from different jobs should then be concatenated using `concatenate_jobs`
    Can concatenate LCI results, supply arrays, and A and B matrices.
    Uses MultiProcessing to work on multiple activities at once."""
    

def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]
    
def concat_vectors_worker(activity_list, output_type, job, 
                          base_dir, database_name, output_folder,
                          delete_raw_files=False):
    """Worker to concatenate and save samples for a given job"""
        
    jobs_samples_folder = os.path.join(base_dir, database_name,
                                       'jobs', job)
    iterations = [folder for folder in glob.glob(jobs_samples_folder+'/*/')
                  if 'concatenated_arrays' not in folder
                  and 'common_files' not in folder]
    nb_iterations = len(iterations)
    for act in activity_list:
        if act in os.listdir(output_folder):
            pass
        else:
            files = [os.path.join(it, output_type, act+'.npy')
                            for it in iterations]
            data = [np.load(file) for file in files]
            arr = np.array(data)
            arr = arr.T
            np.save(file=os.path.join(output_folder, act), arr=arr)
            if delete_raw_files:
                for file in files:
                    os.remove(file)
    return None
    
@click.command()
@click.option('--base_dir', help='Path to directory with jobs', type=str) 
@click.option('--database_name', type=str)
@click.option('--include_inventory', default=True, type=bool)
@click.option('--include_matrices', default=False, type=bool)
@click.option('--include_supply', default=False, type=bool)
@click.option('--cpus', help='Number of CPUs allocated to this work', type=int)
@click.option('--delete_raw_files', help='Delete raw Monte Carlo results after creation of arrays', default=False, type=bool)

def concatenate_within_jobs(base_dir, database_name, include_inventory, include_supply, include_matrices, cpus, delete_raw_files, force_through=False):

    if not any([include_inventory, include_supply, include_matrices]):
        print("No output requested. At least one of the following must be true:")
        print("save_inventory, save_supply or save_matrices")
        sys.exit(0)

    job_dir = os.path.join(base_dir, database_name, 'jobs')
    jobs = glob.glob(job_dir+'/*/')
    logs = {}
    for job in jobs:
        assert os.path.isfile(os.path.join(job, 'log.json')), "Missing log file, run clean_jobs.py first."
        with open(os.path.join(job, 'log.json'), 'r') as f:
            log = json.load(f)
        logs[job] = log

    for log_id, log in logs.items():
        if include_inventory:
            assert log['cleaned']['included_elements']['Inventory'], "Inventory not cleaned for {}, function aborted. Must clean first using clean_jobs.py".format(job)
        if include_supply:
            assert log['cleaned']['included_elements']['Supply'], "Supply arrays not cleaned for {}, function aborted. Must clean first using clean_jobs.py".format(job)
        if include_matrices:
            assert log['cleaned']['included_elements']['Matrices'], "Matrices not cleaned for {}, function aborted. Must clean first using clean_jobs.py".format(job)
    print("Processing jobs: {}".format(jobs))
    for job in jobs:
        jobs_samples_folder = os.path.join(base_dir, database_name,
                                           'jobs', job)
        iterations = [folder for folder in glob.glob(jobs_samples_folder+'/*/')
                      if 'concatenated_arrays' not in folder
                      and 'common_files' not in folder
                      and 'log.json' not in folder]


        if include_inventory:
            output_folder = os.path.join(base_dir, database_name, 'jobs',
                                         job, 'concatenated_arrays', 'Inventory')
            if not os.path.isdir(output_folder):
                os.makedirs(output_folder)

            with open(os.path.join(jobs_samples_folder, 'common_files', 'activity_UUIDs.json'), 'r') as file:
                act_list = json.load(file)
            activity_sublists = chunks(act_list, ceil(len(act_list)/cpus))    
            output_folder = os.path.join(base_dir, 'database_name', 'jobs',
                                     job, 'concatenated_arrays', 'Inventory')
            if not os.path.isdir(output_folder):
                os.makedirs(output_folder)
            workers = []

            for s in activity_sublists:            
                j = mp.Process(target=concat_vectors_worker, 
                               args=(s,
                                     'Inventory',
                                     job,
                                     base_dir, 
                                     database_name,
                                     output_folder,
                                     delete_raw_files
                                     )
                                )
                              
                workers.append(j)
            for w in workers:
                w.start()
            for w in workers:
                w.join()
        if include_supply:
            output_folder = os.path.join(base_dir, 'database_name', 'jobs',
                                         job, 'concatenated_arrays', 'Supply')
            if not os.path.isdir(output_folder):
                os.makedirs(output_folder)

            act_list = [file[:-4] for file in os.listdir(os.path.join(iterations[0], 'Supply'))]
            activity_sublists = chunks(act_list, ceil(len(act_list)/cpus))    
            output_folder = os.path.join(base_dir, 'database_name', 'jobs',
                                     job, 'concatenated_arrays', 'Supply')
            if not os.path.isdir(output_folder):
                os.makedirs(output_folder)
            workers = []

            for s in activity_sublists:            
                j = mp.Process(target=concat_vectors_worker, 
                               args=(s,
                                     'Supply',
                                     job, 
                                     base_dir, 
                                     database_name,
                                     output_folder,
                                     delete_raw_files
                                     )
                                )
                              
                workers.append(j)
            for w in workers:
                w.start()
            for w in workers:
                w.join()
        if include_matrices:
            def process_matrix(matrix):
                files = [os.path.join(it, 'Matrices', matrix+'.npy')
                                for it in iterations]
                data = [np.load(file) for file in files]
                arr = np.array(data)
                arr = arr.T
                output_folder = os.path.join(base_dir, 'database_name', 'jobs',
                                             job, 'concatenated_arrays', 'Matrices')
                if not os.path.isdir(output_folder):
                    os.mkdir(output_folder)

                np.save(file=os.path.join(output_folder, matrix), arr=arr)
                if delete_raw_files:
                    for file in files:
                        os.remove(file)
                return None
            process_matrix('A_matrix')
            process_matrix('B_matrix')
            
        now = datetime.datetime.now()    
        logs[job]['internally_concatenated'] = {
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
            log = json.dump(logs[job], f, indent=4)
    
    print("All requested samples now concatenated within jobs. The next task: concatenate across jobs using concatenate_across_jobs.py")
    return None

if __name__ == "__main__":
    __spec__ = None
    concatenate_within_jobs()
    