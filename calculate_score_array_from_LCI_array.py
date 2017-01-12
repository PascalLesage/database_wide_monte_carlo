import os
import numpy as np
import pickle
from brightway2 import *
import click

def calculate_score_array_from_LCI_array(LCI_array_fp, ref_bio_dict_fp, method, output_dir, dump_only):
    print('\a\a')
    '''Function to calculate a score array from a precalculated LCI array.
        LCI arrays are build with `create_result_arrays`, the latter using the 
        output of `database_wide_monte_carlo` (which ideally has been cleaned
        using `clean_jobs`.
        LCI_array_fp: filepath to the LCI array
        ref_bio_dict: filepath to the reference biodict (i.e. the one used by
                      the LCI array)
        method:       tuple (e.g. ('IPCC 2013', 'GWP', '100 years'))
        output_dir:   filepath root where the resulting array will be dumped. 
    '''
    # Load all the (biosphere exchange, characterization factor) tuples for the given method, in a list
    loaded_method = Method(method).load()

    # Load the reference bio_dict and the LCI_array
    with open(ref_bio_dict_fp, 'rb') as f:
        ref_bio_dict = pickle.load(f)
    LCI_array = np.load(LCI_array_fp)
    
    # Get the biosphere exchange for each (biosphere )
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

    # Create an LCI array that only contains the exchanges that 
    # have characterization factors
    filtered_LCI_array = LCI_array[lca_specific_biosphere_indices][:]
    cf_array = np.reshape(np.array(cfs),(-1,1))
    # Sum of multiplication of inventory result and CF
    LCIA_array = (np.array(filtered_LCI_array)* cf_array).sum(axis=0)

    # Create folder to dump result
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    if not os.path.isdir(os.path.join(output_dir, 'LCIA_arrays')):
        os.mkdir(os.path.join(output_dir, 'LCIA_arrays'))    
    if not os.path.isdir(os.path.join(output_dir, 'LCIA_arrays', Method(method).get_abbreviation())):
        os.mkdir(os.path.join(output_dir, 'LCIA_arrays', Method(method).get_abbreviation()))
    #dump
    LCIA_array.dump(os.path.join(output_dir, 'LCIA_arrays', Method(method).get_abbreviation(), os.path.basename(LCI_array_fp)))
    
    if dump_only == True:
        return None
    else:
        return LCIA_array