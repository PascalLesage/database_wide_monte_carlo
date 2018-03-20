import os
import numpy as np
import pickle
import click
import glob
import shutil
import json
import pandas as pd
from brightway2 import *
import datetime

@click.command()
@click.option('--base_dir', help='Path to directory with jobs', type=str) 
@click.option('--database_name', type=str)
@click.option('--project_name', type=str)
@click.option('--include_inventory', default=True, type=bool)
@click.option('--include_matrices', default=False, type=bool)
@click.option('--include_supply', default=False, type=bool)
@click.option('--delete_temps', help='Delete job-level concatenated files', type=bool)


def concatenate_across_jobs(base_dir, database_name, project_name, 
                            include_inventory, include_supply,
                            include_matrices, delete_temps):
    ''' Concatenates and stores samples from multiple jobs.
        
    This is done **after** samples **within** jobs have been concatenated. 
    Results are stored in a `results` folder.

    '''
    if not any([include_inventory, include_supply, include_matrices]):
        print("No output requested. At least one of the following must be true:")
        print("save_inventory, save_supply or save_matrices")
        sys.exit(0)

    results_folder = os.path.join(base_dir, database_name, 'results')
    if not os.path.isdir(results_folder):
        os.makedirs(results_folder)
    
    job_dir = os.path.join(base_dir, database_name, 'jobs')
    jobs = glob.glob(job_dir+'/*/')
    
    # Move common_files from job[0]: it becomes the "reference" job
    source_dir = os.path.join(jobs[0], 'common_files')
    files_to_move = [os.path.join(source_dir, f) for f in os.listdir(source_dir)]
    reference_folder = os.path.join(results_folder, 'reference_files')
    if not os.path.isdir(reference_folder):
        os.makedirs(reference_folder)
    for file in files_to_move:
        shutil.copy(file, reference_folder)

    # Create ref objects
    ref_A_coo_cols = np.load(os.path.join(reference_folder, 'tech_col_indices.npy'))
    ref_A_coo_rows = np.load(os.path.join(reference_folder, 'tech_row_indices.npy'))
    ref_B_coo_cols = np.load(os.path.join(reference_folder, 'bio_col_indices.npy'))
    ref_B_coo_rows = np.load(os.path.join(reference_folder, 'bio_row_indices.npy'))


    with open(os.path.join(reference_folder, 'bio_dict.pickle'), 'rb') as f:
        ref_bio_dict = pickle.load(f)
    with open(os.path.join(reference_folder, 'activity_dict.pickle'), 'rb') as f:
        ref_activity_dict = pickle.load(f)
    with open(os.path.join(reference_folder, 'product_dict.pickle'), 'rb') as f:
        ref_product_dict = pickle.load(f)
    ref_rev_bio_dict = {v:k for k, v in ref_bio_dict.items()}
    ref_rev_activity_dict = {v:k for k, v in ref_activity_dict.items()}
    ref_rev_product_dict = {v:k for k, v in ref_product_dict.items()}
    with open(os.path.join(reference_folder, 'activity_UUIDs.json'), 'rb') as f:
        activity_UUIDs = json.load(f)
    
    ref_A_indices = {(ref_rev_product_dict[ref_A_coo_rows[i]], ref_rev_activity_dict[ref_A_coo_cols[i]]):i
                    for i in np.arange(ref_A_coo_rows.shape[0])
                    }
    ref_B_indices = {(ref_rev_bio_dict[ref_B_coo_rows[i]], ref_rev_activity_dict[ref_B_coo_cols[i]]):i
                    for i in np.arange(ref_B_coo_rows.shape[0])
                    }
    
    # Make sure all the required files are present and cover the same activities
    for job in jobs:
        assert 'concatenated_arrays' in os.listdir(job), "Jobs missing concatenated arrays folder for job {job}"
        concatenated_dir = os.path.join(job, 'concatenated_arrays')

        if include_inventory:    
            assert 'Inventory' in os.listdir(concatenated_dir), "No inventory results in concatenated folder of job {}, must run concatenate_within_jobs.py first".format(job)
            assert set(activity_UUIDs) == set([act[:-4] for act in os.listdir(os.path.join(job, 'concatenated_arrays', 'Inventory'))]), "The activity lists are not consistent across jobs"
        if include_supply:
            assert 'Supply' in os.listdir(concatenated_dir), "No supply arrays in concatenated folder of job {}, must run concatenate_within_jobs.py first".format(job)
            assert set(activity_UUIDs) == set([act[:-4] for act in os.listdir(os.path.join(job, 'concatenated_arrays', 'Supply'))]), "The activity lists are not consistent across jobs"
        if include_matrices:
            assert 'Matrices' in os.listdir(concatenated_dir), "No matrices in concatenated folder of job {}, must run concatenate_within_jobs.py first".format(job)        
    
    # Function to align arrays from different jobs
    # Only useful if jobs come from different projects
    def translate(arr, ref_dict, rev_dict):
        translator = np.array([ref_dict[rev_dict[row]] for row in rev_dict])
        return arr[translator]

    if include_inventory:
        for act in activity_UUIDs:
            data = []
            for job in jobs:    
                with open(os.path.join(job, 'common_files', 'bio_dict.pickle'), 'rb') as f:
                    bio_dict = pickle.load(f)
                    rev_bio_dict = {k:v for v, k in bio_dict.items()}
                data.append(translate(
                    np.load(os.path.join(job, 'concatenated_arrays', 'Inventory', act+'.npy')),
                    ref_bio_dict,
                    rev_bio_dict))
                if delete_temps:
                    os.remove(os.path.join(job, 'concatenated_arrays', 'Inventory', act+'.npy'))
            if not os.path.isdir(os.path.join(results_folder, 'Inventory')):
                os.makedirs(os.path.join(results_folder, 'Inventory'))
            np.save(
                os.path.join(results_folder, 'Inventory', act),
                np.concatenate(data, axis=1)
                )

    if include_supply:
        for act in activity_UUIDs:
            data = []
            for job in jobs:    
                with open(os.path.join(job, 'common_files', 'activity_dict.pickle'), 'rb') as f:
                    activity_dict = pickle.load(f)
                    rev_activity_dict = {v:k for k, v in activity_dict.items()}
                data.append(translate(
                    np.load(os.path.join(job, 'concatenated_arrays', 'Supply', act+'.npy')),
                    ref_activity_dict,
                    rev_activity_dict))
                if delete_temps:
                    os.remove(os.path.join(job, 'concatenated_arrays', 'Supply', act+'.npy'))
            if not os.path.isdir(os.path.join(results_folder, 'Supply')):
                os.makedirs(os.path.join(results_folder, 'Supply'))
            np.save(
                os.path.join(results_folder, 'Supply', act),
                np.concatenate(data, axis=1)
                )

    if include_matrices:
        def create_A_indices_dict(job, ref_coo_rows, ref_coo_cols):
            with open(os.path.join(job, 'common_files', 'activity_dict.pickle'), 'rb') as f:
                activity_dict = pickle.load(f)
                rev_activity_dict = {v:k for k, v in activity_dict.items()}
            with open(os.path.join(job, 'common_files', 'product_dict.pickle'), 'rb') as f:
                product_dict = pickle.load(f)
                rev_product_dict = {v:k for k, v in product_dict.items()}
            return {(rev_product_dict[ref_coo_rows[i]], rev_activity_dict[ref_coo_cols[i]]):i
                    for i in np.arange(ref_coo_rows.shape[0])
                    }
        data = []
        for job_id, job in enumerate(jobs):
            file = os.path.join(job, 'concatenated_arrays', 'Matrices', 'A_matrix.npy')
            if job_id == 0:
                data.append(np.load(file))
            else:
                A_indices = create_A_indices_dict(job, ref_A_coo_rows, ref_A_coo_cols)
                rev_A_indices = {v:k for k, v in A_indices.items()}
                data.append(translate(np.load(file), ref_A_indices, rev_A_indices))
            if delete_temps:
                os.remove(file)
        if not os.path.isdir(os.path.join(results_folder, 'Matrices')):
            os.makedirs(os.path.join(results_folder, 'Matrices'))
        np.save(
            os.path.join(results_folder, 'Matrices', 'A_matrix'),
            np.concatenate(data, axis=1)
            )

        def create_B_indices_dict(job, ref_coo_rows, ref_coo_cols):
            with open(os.path.join(job, 'common_files', 'activity_dict.pickle'), 'rb') as f:
                activity_dict = pickle.load(f)
                rev_activity_dict = {v:k for k, v in activity_dict.items()}
            with open(os.path.join(job, 'common_files', 'bio_dict.pickle'), 'rb') as f:
                bio_dict = pickle.load(f)
                rev_bio_dict = {v:k for k, v in bio_dict.items()}
            return {(rev_bio_dict[ref_coo_rows[i]], rev_activity_dict[ref_coo_cols[i]]):i
                    for i in np.arange(ref_coo_rows.shape[0])
                    }
        data = []
        for job_id, job in enumerate(jobs):
            file = os.path.join(job, 'concatenated_arrays', 'Matrices', 'B_matrix.npy')
            if job_id == 0:
                data.append(np.load(file))
            else:
                B_indices = create_B_indices_dict(job, ref_B_coo_rows, ref_B_coo_cols)
                rev_B_indices = {v:k for k, v in B_indices.items()}
                data.append(translate(np.load(file), ref_B_indices, rev_B_indices))
            if delete_temps:
                os.remove(file)
        np.save(
            os.path.join(results_folder, 'Matrices', 'B_matrix'),
            np.concatenate(data, axis=1)
            )
    
    # Update the job logs
    for job in jobs:
        try:
            with open(os.path.join(job, 'log.json'), 'rb') as f:
                log = json.load(f)
        except:
            log = {}
        now = datetime.datetime.now() 
        log['included_in_global_concatenated_results'] = {
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

    # Generate some nice Excel files to make it easier to use output
    # Useful activity description
    projects.set_current(project_name)
    
    cols = ['name', 'location', 'reference product', 'production amount', 'unit']
    file = os.path.join(reference_folder, 'activity_UUIDs.json')
    with open(file, "r") as f:
        activity_UUIDs = json.load(f)
    df = pd.DataFrame(index=activity_UUIDs, columns=cols)
    for act_UUID in activity_UUIDs:
        act = get_activity((database_name, act_UUID))
        for field in cols:
            df.loc[act_UUID, field] = act[field]
    df.to_excel(os.path.join(reference_folder, 'activity_details.xlsx'))
    
    # Useful parameter mapping: A matrix
    df = pd.DataFrame(columns=['row_indices', 'col_indices'])
    df['row_indices']=ref_A_coo_rows
    df['col_indices']=ref_A_coo_cols
    df['input_database']=df['row_indices'].apply(lambda x: ref_rev_product_dict[x][0])
    df['input_code']=df['row_indices'].apply(lambda x: ref_rev_product_dict[x][1])
    df['output_database']=df['col_indices'].apply(lambda x: ref_rev_activity_dict[x][0])
    df['output_code']=df['col_indices'].apply(lambda x: ref_rev_activity_dict[x][1])
    df.to_excel(os.path.join(reference_folder, 'A_indices_mapping.xlsx'))
    
    # Useful parameter mapping: B matrix
    df = pd.DataFrame(columns=['row_indices', 'col_indices'])
    df['row_indices']=ref_B_coo_rows
    df['col_indices']=ref_B_coo_cols
    df['input_database']=df['row_indices'].apply(lambda x: ref_rev_bio_dict[x][0])
    df['input_code']=df['row_indices'].apply(lambda x: ref_rev_bio_dict[x][1])
    df['output_database']=df['col_indices'].apply(lambda x: ref_rev_activity_dict[x][0])
    df['output_code']=df['col_indices'].apply(lambda x: ref_rev_activity_dict[x][1])
    df.to_excel(os.path.join(reference_folder, 'B_indices_mapping.xlsx'))
    
    # Useful inventory row mapping
    cols = ['database', 'code', 'name', 'compartment', 'subcompartment', 'unit']
    df = pd.DataFrame(columns=cols)
    df.index.name = 'index'
    for i in np.arange(len(ref_bio_dict)):
        ef = get_activity(ref_rev_bio_dict[i])
        df.loc[i, 'database'] = ef.key[0]
        df.loc[i, 'code'] = ef.key[1]
        df.loc[i, 'name'] = ef['name']
        df.loc[i, 'unit'] = ef['unit']
        df.loc[i, 'compartment'] = ef['categories'][0]
        try:
            df.loc[i, 'subcompartment'] = ef['categories'][1]
        except:
            df.loc[i, 'subcompartment'] = None
    df.to_excel(os.path.join(reference_folder, 'inventory_indices_mapping.xlsx'))    

    # Useful supply array row mapping
    cols = ['database', 'code', 'name', 'location', 'unit']
    df = pd.DataFrame(columns=cols)
    df.index.name = 'index'
    for i in np.arange(len(ref_activity_dict)):
        act = get_activity(ref_rev_activity_dict[i])
        df.loc[i, 'database'] = act.key[0]
        df.loc[i, 'code'] = act.key[1]
        df.loc[i, 'name'] = act['name']
        df.loc[i, 'unit'] = act['unit']
        df.loc[i, 'location'] = act['location']
    df.to_excel(os.path.join(reference_folder, 'supply_array_indices_mapping.xlsx'))    

    # Generate a useful Excel to get information about methods
    method_list = list(methods)
    m_method=[m[0] for m in method_list]
    m_IC1=[m[1] for m in method_list]
    m_IC2=[m[2] for m in method_list]
    m_Unit=[Method(m).metadata['unit'] for m in method_list]
    m_MD5hash=[Method(m).get_abbreviation() for m in method_list]
    df = pd.DataFrame.from_items(
        [
            ('Method', m_method),
            ('Impact category (1)', m_IC1),
            ('Impact category (2)', m_IC2),
            ('Unit', m_Unit),
            ('MD5 hash', m_MD5hash),
            ('Brightway compliant name', method_list)
        ]
    )
    df = df.set_index('MD5 hash')
    df.to_excel(os.path.join(results_folder, 'reference_files', 'methods description.xlsx'))
    
    job_logs = {}
    for job in jobs:
        with open(os.path.join(job, 'log.json'), 'rb') as f:
                log = json.load(f)
        job_logs[str(job)] = log
    now = datetime.datetime.now()     
    result_log = {
        'concatenated_accross_jobs': {
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
                            now.minute),
                        'included_jobs': job_logs
            }
        }
    with open(os.path.join(results_folder, 'log.json'), 'w') as f:
                log = json.dump(result_log, f, indent=4)       
    
    print("Requested arrays successfully concatenated and saved to results")
    return None
    
if __name__=='__main__':
    concatenate_across_jobs()