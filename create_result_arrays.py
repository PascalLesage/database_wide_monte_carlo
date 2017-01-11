import os
import shutil
import numpy as np
import pickle
import time
import multiprocessing as mp
from math import ceil
import click

""" For a list of MC jobs, generate result arrays.
    Results arrays refer to supply arrays or LCIs."""

def create_arrays_worker(activity_list,
                         output_dir,
                         worker_id,
                         delete_raw_files):

    with (open(os.path.join(output_dir,'background_files','activity_dict.pickle'), 'rb')) as f:
        reference_activity_dict = pickle.load(f)
    with (open(os.path.join(output_dir,'background_files','bio_dict.pickle'), 'rb')) as f:
        reference_bio_dict = pickle.load(f)
    with (open(os.path.join(output_dir,'background_files','ordered_job_fp_list.pickle'), 'rb')) as f:
        ordered_job_fp_list = pickle.load(f)
    
    done = 1  # Simply for printing out progress

    for activity in activity_list:
        print("worker {} treating {} of {}".format(worker_id,
                                                   done,
                                                   len(activity_list)
                                                   ))


        # Create numpy arrays where the all MC results will be stored
        # Supply arrays
        supply_arr = np.empty((len(reference_activity_dict), 0))
        # LCI arrays
        LCI_arr = np.empty((len(reference_bio_dict), 0))
        # If the jobs were done from different BW projects, the activity_dicts and
        # bio_dicts will not be the same (i.e. the order of the activities and 
        # of the biosphere flows will be different from job to job). 
        # One needs to create an index for changing order of activities/biosphere flows.
        
        rev_activity_dict_ref = {v: k for k, v in reference_activity_dict.items()}
        ref_activity_order = [rev_activity_dict_ref[exc]
                    for exc in range(len(rev_activity_dict_ref))]
        rev_bio_dict_ref = {v: k for k, v in reference_bio_dict.items()}
        ref_bio_order = [rev_bio_dict_ref[exc]
                    for exc in range(len(rev_bio_dict_ref))]

        for current_job in ordered_job_fp_list:
            # Load the activity_dict relevant for the current job
            current_activity_dict = pickle.load(
                                open(os.path.join(current_job[0],
                                                  [fp for fp in os.listdir(
                                                      current_job[0])
                                                if 'common_files' in fp][0],
                                    'activity_dict.pickle'), 'rb'))

            # Check if the activity_dict is the same as the reference activity_dict
            activity_same = (current_activity_dict == reference_activity_dict)
            # If not the same, then reindexing of arrays will be necessary.
            # Create reindexer to rearrange the elements of the supply arrays.
            if not activity_same:
                act_reindexer = np.array([current_activity_dict[exc]
                                        for exc in ref_activity_order])

            # Load the bio_dict relevant for the current job                                        
            current_bio_dict = pickle.load(
                                open(os.path.join(current_job[0],
                                                  [fp for fp in os.listdir(
                                                      current_job[0])
                                                if 'common_files' in fp][0],
                                    'bio_dict.pickle'), 'rb'))
            # Check if the bio_dict is the same as the reference bio_dict
            bio_same = (current_bio_dict == reference_bio_dict)
            # If not the same, then reindexing of arrays will be necessary.
            # Create reindexer to rearrange the elements of the LCI arrays.
            if not bio_same:
                bio_reindexer = np.array([current_bio_dict[exc]
                                        for exc in ref_bio_order])

            # Create empty arrays to collect job results
            current_supply_arr = np.empty((len(reference_activity_dict), 0))
            current_LCI_arr = np.empty((len(reference_bio_dict), 0))
            
            # Create a list of iteration folder paths
            iteration_fps = current_job[1]
    
            # Iterate through iteration folders
            for it_id, it in enumerate(iteration_fps):
                np_file = "{}.npy".format(activity)
                
                supply_folder = [fp for fp in os.listdir(it) if 'Supply Arrays' in fp][0]
                current_supply_arr_it = np.load(os.path.join(it,supply_folder,np_file))
                current_supply_arr_it = np.reshape(current_supply_arr_it, (-1,1))
                current_supply_arr = np.concatenate((current_supply_arr,
                                              current_supply_arr_it), axis = 1)

                LCI_folder = [fp for fp in os.listdir(it) if 'Inventory' in fp][0]
                current_LCI_arr_it = np.load(os.path.join(it,LCI_folder,np_file))
                current_LCI_arr_it = np.reshape(current_LCI_arr_it, (-1,1))
                current_LCI_arr = np.concatenate((current_LCI_arr,
                                              current_LCI_arr_it), axis = 1)
            
            # The data for the given activity and job is now all in 
            # current_arr, and can be concatenated to the general array for 
            # the activity of interest. The current_arr needs to be reindexed
            # if the activity_dicts were different.
            if not activity_same:
                current_supply_arr = current_supply_arr[act_reindexer]
            supply_arr = np.concatenate((supply_arr, current_supply_arr), axis=1)

            if not bio_same:
                current_LCI_arr = current_LCI_arr[bio_reindexer]
            LCI_arr = np.concatenate((LCI_arr, current_LCI_arr), axis=1)

            
        # Dump the arrays to disk for future use
        supply_arr.dump(os.path.join(output_dir, 'supply_arrays', '{}.npy'.format(activity)))
        LCI_arr.dump(os.path.join(output_dir, 'LCI_arrays', '{}.npy'.format(activity)))
        
        if delete_raw_files == True:
            for current_job in ordered_job_fp_list:
                for it in current_job[1]:
                    supply_folder = [fp for fp in os.listdir(it) if 'Supply Arrays' in fp][0]
                    os.remove(os.path.join(it, supply_folder, activity+'.npy'))
                    LCI_folder = [fp for fp in os.listdir(it) if 'Inventory' in fp][0]
                    os.remove(os.path.join(it, LCI_folder, activity+'.npy'))
        done += 1
        
def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]

@click.command()
@click.option('--job_dir', help='Path to directory with jobs', type=str)
@click.option('--output_dir', help='Path to directory where resulting arrays should be stored', type=str)
@click.option('--cpus', help='Number of CPUs allocated to this work', type=int)
@click.option('--delete_raw_files', help='Delete raw Monte Carlo results after creation of arrays', type=bool)


def main(job_dir, output_dir, cpus, delete_raw_files):
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    for subdir in ['background_files', 'supply_arrays', 'LCI_arrays', 'LCIA_arrays', 'sampled_parameter_values']:
        try:
            os.mkdir(os.path.join(output_dir, subdir))
        except:
            pass
    
    job_list = [os.path.join(job_dir, fp)
                for fp in os.listdir(job_dir)
                if os.path.isdir(os.path.join(job_dir, fp))]
    
    reference_common_files_path = [os.path.join(job_list[0], path) for path in os.listdir(job_list[0]) if "common_files" in path][0]
    for file in os.listdir(reference_common_files_path):
        shutil.copyfile(os.path.join(reference_common_files_path, file), os.path.join(output_dir, 'background_files', file))
    
    
    #reference_activity_dict_path = os.path.join(reference_common_files_path, "activity_dict.pickle")
    #reference_activity_dict = pickle.load(open(reference_activity_dict_path,'rb'))

    random_result_folder_fp = [os.path.join(job_list[0], path) for path in os.listdir(job_list[0]) if "common_files" not in path][0]
    random_LCI_result_folder_fp = [os.path.join(random_result_folder_fp, path) for path in os.listdir(random_result_folder_fp) if "Inventory" in path][0]
    activity_list = [act[:-4] for act in os.listdir(random_LCI_result_folder_fp)]
    with open(os.path.join(output_dir, 'background_files', 'activity_list.pickle'), "wb") as f:
        pickle.dump(activity_list, f)

    # The order of iterations in arrays is not important, but it must be constant
    # across all arrays. These two lists, stored as pickle files, will help ensure this.
    ordered_job_fp_list = []
    
    for job in job_list:
        job_iteration_fps = [os.path.join(job, folder) for folder in os.listdir(job) if "common_files" not in folder]
        ordered_job_fp_list += [[job, job_iteration_fps]]
    
    with open(os.path.join(output_dir, 'background_files', 'ordered_job_fp_list.pickle'), "wb") as f:
        pickle.dump(ordered_job_fp_list, f)

    already_treated = [file[:-4] for file in os.listdir(os.path.join(output_dir, 'LCI_arrays'))] # Supposes LCI arrays get generated last!
    to_treat = [file for file in activity_list if file not in already_treated]
    
    print("Total activities: {}, altready treated: {}, to treat: {}".format(len(activity_list), len(already_treated), len(to_treat)))
    
    activity_sublists = chunks(to_treat, ceil(len(to_treat)/cpus))
    
    workers = []

    for i, s in enumerate(activity_sublists):            
        j = mp.Process(target=create_arrays_worker, 
                       args=(s,
                             output_dir,
                             i,
                             delete_raw_files
                             )
                        )
        
        workers.append(j)
    for w in workers:
        w.start()
        
if __name__ == "__main__":
    main()