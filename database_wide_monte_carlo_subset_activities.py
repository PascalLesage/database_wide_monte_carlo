from brightway2 import *
import numpy as np
import os
import multiprocessing as mp
from scipy.sparse.linalg import factorized, spsolve
from scipy import sparse
import datetime
import pickle
import click

"""
Used to generate uncertainty information at the database level.
For each iteration:
- New values for uncertain parameters of the technosphere (A) and biosphere (B) matrices are generated
- Cradle-to-gate LCI results are calculated for all potential output of the LCI database

The following is stored in a specified directory: 
- All values of the A and B matrices
- For each functional unit: 
    - the supply array (aka the scaling vector)
    - the life cycle inventory
""" 

class DirectSolvingPVLCA(ParameterVectorLCA, DirectSolvingMixin):
    pass

def worker_process(project, job_dir, job_id, worker_id, functional_units, iterations):

    projects.set_current(project)
    collector_functional_unit = {k:v for d in functional_units for k, v in d.items()}
    lca = DirectSolvingPVLCA(demand = collector_functional_unit)
    lca.load_data()
        
    for index in range(iterations):
    
        print('--Starting job for worker {}, iteration {}'.format(worker_id, index))
        
        #Creating directories for current iteration
        it_nb_worker_id = "{}_iteration_{}-{}".format(job_id,worker_id,index)
        index_dir = os.path.join(job_dir, it_nb_worker_id)
        os.mkdir(index_dir)
                
        supplyArray_dir = os.path.join(index_dir, '{}-{}-Supply Arrays'.format(worker_id, index))
        os.mkdir(supplyArray_dir)
        
        inventory_dir = os.path.join(index_dir, '{}-{}-Inventory'.format(worker_id, index))
        os.mkdir(inventory_dir)
        
        lca.rebuild_all()

        #Saving Samples for current iteration
        np.save(os.path.join(index_dir, "Sample.npy"), np.array(lca.sample, dtype = np.float32))

        lca.decompose_technosphere()
        
        for act_index, fu in enumerate(functional_units):
            
            #Creating UUID for each activity
            #actKey = list(fu.keys())[0][1]
            actKey = str(list(fu.keys())[0])
            
            lca.build_demand_array(fu)

            lca.supply_array = lca.solve_linear_system()
            np.save(os.path.join(supplyArray_dir, "{}.npy".format(actKey)), np.array(lca.supply_array, dtype = np.float32))

            count = len(lca.activity_dict)
            lca.inventory = lca.biosphere_matrix * sparse.spdiags([lca.supply_array], [0], count, count)
            np.save(os.path.join(inventory_dir, "{}.npy".format(actKey)), np.array(lca.inventory.sum(1), dtype = np.float32))
    

def get_useful_info(activities, collector_functional_unit, BASE_OUTPUT_DIR, job_id):

    # Random sacrificial LCA to extract relevant information
    sacrificial_lca = LCA(collector_functional_unit)
    sacrificial_lca.lci()

    # Folder containing information
    common_dir = os.path.join(BASE_OUTPUT_DIR, job_id, "{}_common_files".format(job_id))
    os.mkdir(common_dir)
    
    with open(os.path.join(common_dir, 'activities.pickle'), "wb") as f:
        pickle.dump(activities, f)
    
    with open(os.path.join(common_dir, 'product_dict.pickle'), "wb") as f:
        pickle.dump(sacrificial_lca.product_dict, f)
    
    with open(os.path.join(common_dir,'bio_dict.pickle'), "wb") as f:
        pickle.dump(sacrificial_lca.biosphere_dict, f)
    
    with open(os.path.join(common_dir,'activity_dict.pickle'), "wb") as f:
        pickle.dump(sacrificial_lca.activity_dict, f)

    rev_activity_dict, rev_product_dict, rev_bio_dict = sacrificial_lca.reverse_dict()
    
    with open(os.path.join(common_dir,'rev_product_dict.pickle'), "wb") as f:
        pickle.dump(rev_product_dict, f)
    
    with open(os.path.join(common_dir,'rev_activity_dict.pickle'), "wb") as f:
        pickle.dump(rev_activity_dict, f)
    
    with open(os.path.join(common_dir,'rev_bio_dict.pickle'), "wb") as f:
        pickle.dump(rev_bio_dict, f)
    
    with open(os.path.join(common_dir,'tech_params.pickle'), "wb") as f:
        pickle.dump(sacrificial_lca.tech_params, f)
    
    with open(os.path.join(common_dir,'bio_params.pickle'), "wb") as f:
        pickle.dump(sacrificial_lca.bio_params, f)
    
    with open(os.path.join(common_dir,'IO_Mapping.pickle'), "wb") as f:
        pickle.dump({v:k for k,v in mapping.items()}, f)

    return None


@click.command()
@click.option('--project', default='default', help='Brightway2 project name', type=str)
@click.option('--activity_list_fp', help='Filepath to pickled list of activity keys.', type=str)
@click.option('--iterations', default=1000, help='Number of Monte Carlo iterations', type=int)
@click.option('--cpus', default=mp.cpu_count(), help='Number of used CPU cores', type=int)
@click.option('--output_dir', help='Output directory path', type=str)

def main(project, activity_list_fp, iterations, cpus, output_dir):

    projects.set_current(project)
    bw2setup()
    
    BASE_OUTPUT_DIR = output_dir
    
    now = datetime.datetime.now()
    job_id = "{}_{}-{}-{}_{}h{}".format(os.environ['COMPUTERNAME'],now.year, now.month, now.day, now.hour, now.minute)
        
    with open(activity_list_fp, 'rb') as f:
        activities = pickle.load(f)
    functional_units = [ {get_activity(act).key: 1} for act in activities ]
    collector_functional_unit = {k:v for d in functional_units for k, v in d.items()}
    
    os.chdir(BASE_OUTPUT_DIR)
    job_dir = os.path.join(BASE_OUTPUT_DIR, job_id)
    os.mkdir(job_dir)
    
    get_useful_info(activities, collector_functional_unit, BASE_OUTPUT_DIR, job_id)
    
    workers = []

    for worker_id in range(cpus):
        # Create child processes that can work apart from parent process
        child = mp.Process(target=worker_process, args=(projects.current, job_dir, job_id, worker_id, functional_units, iterations))
        workers.append(child)
        child.start()

if __name__ == '__main__':
    main()


