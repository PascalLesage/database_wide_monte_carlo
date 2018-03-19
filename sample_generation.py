""" Generation of samples for all activities in an LCI database

Built on the Brightway2 framework.
Stores each result (supply array, inventory vector, sampled matrices) 
for each activity and each iteration is stored as an individual file.
These must then be assembled to be useful.
"""

from brightway2 import *
import numpy as np
import os
import click
import datetime
import multiprocessing as mp
import pickle
import sys
import json

__author__ = "Pascal Lesage"
__credits__ = ["Pascal Lesage, Chris Mutel, Nolwenn Kazoum"]
__license__ = "BSD 3-Clause 'New' or 'Revised' License"
__version__ = "1.1"
__maintainer__ = "Pascal Lesage"
__email__ = "pascal.lesage@polymtl.ca"


class direct_solving_MC(MonteCarloLCA, DirectSolvingMixin):
    ''' Class expanding MonteCarloLCA to include `solve_linear_system`.
    '''
    pass

def correlated_MCs_worker(project_name,
                          job_dir,
                          job_id,
                          worker_id,
                          functional_units_list,
                          iterations,
                          include_inventory,
                          include_supply,
                          include_matrices
                         ):
    '''Generate database-wide correlated Monte Carlo samples
    
    This function is a worker function. It is called from 
    the `generate_samples` function, that dispatches the Monte Carlo 
    work to a specified number of workers.
    '''
    
    # Open the project containing the target database
    projects.set_current(project_name)
    # Create a factice functional unit that spans all possible demands
	# Useful if some activities link to other upstream databases

    collector_functional_unit = {k:v 
                                 for d in functional_units_list 
                                 for k, v in d.items()
                                }
    # Create an LCA object that spans all demands
    lca = direct_solving_MC(demand = collector_functional_unit)
    # Build technosphere and biosphere matrices and corresponding rng
    lca.load_data()
    
    for index in range(iterations):        
        # Make directories for current iteration
        it_nb_worker_id = "iteration_{}-{}".format(worker_id,index)
        index_dir = os.path.join(job_dir, it_nb_worker_id)
        os.mkdir(index_dir)

        # Sample new values for technosphere and biosphere matrices 
        lca.rebuild_technosphere_matrix(lca.tech_rng.next())
        lca.rebuild_biosphere_matrix(lca.bio_rng.next())
                
        if include_matrices:
            matrices_dir = os.path.join(index_dir,'Matrices')
            os.mkdir(matrices_dir)
            np.save(
                os.path.join(matrices_dir, "A_matrix"),
                lca.technosphere_matrix.tocoo().data.astype(np.float32)
                )
            np.save(
                os.path.join(matrices_dir, "B_matrix"),
                lca.biosphere_matrix.tocoo().data.astype(np.float32)
                )

        if any([include_inventory, include_supply]):
            # Factorize technosphere matrix, creating a solver
            lca.decompose_technosphere()
            # For all activities, calculate and save 
            # supply and inventory vectors
            
            for fu in functional_units_list:
                actKey = str(list(fu.keys())[0][1])
                lca.build_demand_array(fu)                
                lca.supply_array = lca.solve_linear_system()

                # Supply arrays
                if include_supply:
                    supply_dir = os.path.join(index_dir,'Supply')
                    if not os.path.isdir(supply_dir):
                        os.makedirs(supply_dir)
                    np.save(
                        os.path.join(supply_dir, actKey),
                        np.array(lca.supply_array, dtype = np.float32)
                    )

                # Inventory

                if include_inventory:
                    inventory_dir = os.path.join(index_dir,'Inventory')
                    if not os.path.isdir(inventory_dir):
                        os.makedirs(inventory_dir)
                    lca.inventory = lca.biosphere_matrix * lca.supply_array
                    np.save(
                        os.path.join(inventory_dir, actKey),
                        np.array(lca.inventory, dtype = np.float32)
                        )
    print(
        "Worker {} finished {} iterations".format(
            worker_id, 
            iterations
            )
        )

def get_useful_info(collector_functional_unit, job_dir, activities, database_name):
    """Collect and save job-level data"""
    
    # Generate sacrificial LCA whose attributes will be saved
    sacrificial_lca = LCA(collector_functional_unit)
    sacrificial_lca.lci()

    # Make folder to contain extracted information
    common_dir = os.path.join(job_dir, 'common_files')
    os.mkdir(common_dir)
    
	# Save various attributes for eventual reuse in interpretation
    file = os.path.join(common_dir, 'activity_UUIDs.json')
    with open(file, "w") as f:
        json.dump(activities, f, indent=4)
        
    fp = os.path.join(common_dir, 'product_dict.pickle')
    with open(fp, "wb") as f:
        pickle.dump(sacrificial_lca.product_dict, f)

    fp = os.path.join(common_dir, 'bio_dict.pickle')
    with open(fp, "wb") as f:
        pickle.dump(sacrificial_lca.biosphere_dict, f)
        
    fp = os.path.join(common_dir, 'activity_dict.pickle')
    with open(fp, "wb") as f:
        pickle.dump(sacrificial_lca.activity_dict, f)
    
    fp = os.path.join(common_dir, 'tech_params.pickle')
    with open(fp, "wb") as f:
        pickle.dump(sacrificial_lca.tech_params, f)
    
    fp = os.path.join(common_dir, 'bio_params.pickle')
    with open(fp, "wb") as f:
        pickle.dump(sacrificial_lca.bio_params, f)
    
    fp = os.path.join(common_dir, 'IO_Mapping.pickle')
    with open(fp, "wb") as f:
        pickle.dump({v:k for k,v in mapping.items()}, f)
        
    fp = os.path.join(common_dir, 'tech_row_indices')
    np.save(fp, sacrificial_lca.technosphere_matrix.tocoo().row)
    
    fp = os.path.join(common_dir, 'tech_col_indices')
    np.save(fp, sacrificial_lca.technosphere_matrix.tocoo().col)
    
    fp = os.path.join(common_dir, 'bio_row_indices')
    np.save(fp, sacrificial_lca.biosphere_matrix.tocoo().row)

    fp = os.path.join(common_dir, 'bio_col_indices')
    np.save(fp, sacrificial_lca.biosphere_matrix.tocoo().col)
    
    return None
            
@click.command()
@click.option('--project_name', default='default', help='Brightway2 project name', type=str)
@click.option('--database_name', help='Database name', type=str)
@click.option('--iterations', default=1000, help='Number of Monte Carlo iterations', type=int)
@click.option('--cpus', default=mp.cpu_count(), help='Number of used CPU cores', type=int)
@click.option('--base_dir', help='Base directory path for precalculated samples', type=str)
@click.option('--include_inventory', help='Save inventory vector', default=True, type=bool)
@click.option('--include_supply', help='Save supply vector', default=False, type=bool)
@click.option('--include_matrices', help='Save A and B matrices', default=False, type=bool)

def generate_samples_job(project_name, database_name, iterations, cpus, base_dir, include_inventory=False, include_supply=False, include_matrices=False):
    """Parent function for database-wide sample generation 
	
	Arguments: 
	project_name -- Brightway2 project where the database is saved (str)
	database_name -- Database name (str)
	iterations -- Number of Monte Carlo iterations required
	cpus -- Number of cpus over which the work is to be distributed
	base_dir -- Root directory for all presampling files
	include_supply -- If True, save the supply vector. Careful: supply vectors take lots of memory. 
    include_matrices -- If True, save A and B matrices
	
	Does not return anything, but saves files in a "job" folder.
	
	Note: The use of @click allows the function arguments to be passed 
	from a command line, but imposes project and database names with no 
	white spaces.
	
	"""
    
    if not any([include_inventory, include_supply, include_matrices]):
        print("No output requested. At least one of the following must be true:")
        print("include_inventory, include_supply or include_matrices")
        sys.exit(0)
    
    # Open the Brighway2 project
    assert project_name in projects, "The requested project does not exist"
    projects.set_current(project_name)
    
	# Create a unique job name
    now = datetime.datetime.now()
    try:    # Works with Linux    
        job_id = "{}_{}-{}-{}_{}h{}".format(
            os.environ['USER'],
            now.year,
            now.month,
            now.day,
            now.hour,
            now.minute
            )
    except:
        try: #Works with Windows
            job_id = "{}_{}-{}-{}_{}h{}".format(
                os.environ['COMPUTERNAME'],
                now.year,
                now.month,
                now.day,
                now.hour,
                now.minute
                )
        except:
            job_id = "{}_{}-{}-{}_{}h{}".format(
                'job',
                now.year,
                now.month,
                now.day,
                now.hour,
                now.minute
                )
            
            
	# Identify all activities for which samples are required
    db = Database(database_name)
    activities = [activity.key[1] for activity in db]
    
    # Specify all functional units
    functional_units = [ {(database_name, act): 1} for act in activities ]
    
	# Make directory to store results
    samples_dir = os.path.join(base_dir, database_name, 'jobs')
    if not os.path.isdir(samples_dir):
        os.makedirs(samples_dir)
    job_dir = os.path.join(samples_dir, job_id)
    os.makedirs(job_dir)

	# Generate and save job-level information
    collector_functional_unit = {k:v for d in functional_units for k, v in d.items()}
    get_useful_info(collector_functional_unit, job_dir, activities, database_name)
    
	# Calculate number of iterations per worker.
    it_per_worker = [iterations//cpus for _ in range(cpus)]
    for _ in range(iterations-cpus*(iterations//cpus)):
        it_per_worker[_]+=1

	# Dispatch actual sampling work to workers
    workers = []
    for worker_id in range(cpus):
        child = mp.Process(target=correlated_MCs_worker,
                           args=(project_name,
								 job_dir,
								 job_id,
								 worker_id,
								 functional_units,
								 it_per_worker[worker_id],
								 include_inventory,
                                 include_supply,
                                 include_matrices
								 )
                           )
        workers.append(child)
        child.start()
    for c in workers:
        c.join()
    
    now = datetime.datetime.now()
    log = {'samples_generated':
            {
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
          }
    with open(os.path.join(job_dir, 'log.json'), 'w') as f:
        json.dump(log, f, indent=4)
        
    print("{} samples generated for {} activities, saved to directory {}.".format(iterations, len(activities), job_dir)) 
    print("Use `clean_jobs.py` to sanitize the data, and then `concatenate_within_jobs.py` to consolidate samples")
    print("See log file for more information")
    
if __name__ == '__main__':
    __spec__ = None
    generate_samples_job()