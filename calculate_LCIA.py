import os
import numpy as np
import pandas as pd
import pickle
from brightway2 import Method, projects, methods
import click
from math import ceil
import multiprocessing as mp

def calculate_score_array_from_LCI_array(results_folder,
                                         lca_specific_biosphere_indices, cfs,
                                         act, output_dir):
    ''' Calculate a score array from a precalculated LCI array.
	
    '''

    LCI_array = np.load(os.path.join(results_folder, 'Inventory', act))
    
    # Create an LCI array that only contains the exchanges that 
    # have characterization factors
    filtered_LCI_array = LCI_array[lca_specific_biosphere_indices][:]
    cf_array = np.reshape(np.array(cfs),(-1,1))
    # Sum of multiplication of inventory result and CF
    LCIA_array = (np.array(filtered_LCI_array)* cf_array).sum(axis=0)
    np.save(os.path.join(output_dir, act), LCIA_array)

    return None

def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def whole_method_LCIA_calculator(method_list, results_folder, ref_bio_dict):
    
    LCI_arrays_dir = os.path.join(results_folder, 'Inventory')
    assert os.path.isdir(LCI_arrays_dir), "No LCI results to process"
    
    LCI_arrays = os.listdir(LCI_arrays_dir)
    
    for method in method_list:
        method_abbreviation = Method(method).get_abbreviation()
        LCIA_folder = os.path.join(results_folder, 'LCIA', method_abbreviation)
        if not os.path.isdir(LCIA_folder):
            os.makedirs(LCIA_folder)

        loaded_method = Method(method).load()
        method_ordered_exchanges = [exc[0] for exc in loaded_method]

        # Collectors for the LCI array indices and characterization factors that are relevant 
        # for the impact assessment (i.e. those that have characterization factors for the given method)
        lca_specific_biosphere_indices = []
        cfs = []
        
        for exc in method_ordered_exchanges: # For every exchange that has a characterization factor
            try:
                lca_specific_biosphere_indices.append(ref_bio_dict[exc]) # Check to see if it is in the bio_dict 
                                                                     # If it is, it is in the inventory
                                                                     # And its index is bio_dict[exc]
                cfs.append(dict(loaded_method)[exc])                 # If it is in bio_dict, we need its
                                                                     # characterization factor
            except:
                pass
        
        for act in LCI_arrays:
            if act in os.listdir(LCIA_folder):
                pass
            else:
                calculate_score_array_from_LCI_array(
                    results_folder,
                    lca_specific_biosphere_indices, cfs,
                    act, LCIA_folder)
    return None

@click.command()
@click.option('--base_dir', help='Path to directory with jobs', type=str) 
@click.option('--project', help='Name of Brightway2 project', type=str)
@click.option('--database_name', type=str)
@click.option('--cpus', help='Number of CPUs allocated to this work', type=int)

def dispatch_LCIA_calc_to_workers(base_dir, project, database_name, cpus):
    projects.set_current(project)
    try:
        method_list = pickle.load(open(os.path.join(base_dir, database_name, 'results', 'reference_files', 'methods.pickle'), 'rb'))
    except:
        method_list = list(methods)
    
    results_folder = os.path.join(base_dir, database_name, 'results')
    
    # Generate a useful Excel to get information about methods
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
            ('MD5 hash', m_MD5hash)
        ]
    )
    df = df.set_index('MD5 hash')
    df.to_excel(os.path.join(results_folder, 'reference_files', 'methods description.xlsx'))
    
    method_sublists = chunks(method_list, ceil(len(method_list)/cpus))
    
    with open(os.path.join(results_folder, 'reference_files', 'bio_dict.pickle'), 'rb') as f:
        ref_bio_dict = pickle.load(f)
    
    workers = []

    for m in method_sublists:            
        j = mp.Process(target=whole_method_LCIA_calculator, 
                       args=(m,
                             results_folder,
                             ref_bio_dict
                             )
                        )
                      
        workers.append(j)
    for w in workers:
        w.start()
        w.join()
    
    
if __name__ == '__main__':
    dispatch_LCIA_calc_to_workers()