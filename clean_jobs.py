
# coding: utf-8

import os
import shutil
import glob
import click

@click.command()
@click.option('--root_dir', help='Path to directory with jobs', type=str)
@click.option('--database_size', help='Number of activities in database', type=int)

def clean_jobs(root_dir, database_size):
    print(database_size)
    print(database_size==4087)
    os.chdir(root_dir)

    jobs = glob.glob(root_dir+'/*/')

    jobs_to_delete = []
    iterations_to_delete = []
    
    for job in jobs:
        job_folders = glob.glob(job+'/*/')
        
        for job_folder in job_folders:
            if "common_files" in job_folder:
                if len(os.listdir(job_folder)) != 10:
                    print("job to be deleted: {}, because it had {} files".format(job, len(os.listdir(job_folder))))
                    jobs_to_delete.append(job)
            else:
                if len(os.listdir(job_folder)) != 3:
                    iterations_to_delete.append(job_folder)
                result_folders = [os.path.join(job_folder,o) for o in os.listdir(job_folder) if os.path.isdir(os.path.join(job_folder,o))] 
                for result_folder in result_folders:
                    if len(os.listdir(result_folder)) != database_size:
                        print("iteration to be deleted: {}, because it had {} files".format(job_folder, len(os.listdir(result_folder))))
                        iterations_to_delete.append(job_folder)

    print("will delete following jobs: {}".format(jobs_to_delete))
    print("will delete the following iterations: {}".format(iterations_to_delete))

    c = input("continue (y/n)")
    if c == "n":
        exit()
    
    for iteration in iterations_to_delete:
        shutil.rmtree(iteration, ignore_errors=True)
        #os.rmdir(iteration)
    for job in jobs_to_delete:
        shutil.rmtree(job, ignore_errors=True)
        #os.rmdir(job)
if __name__ == '__main__':
    clean_jobs()