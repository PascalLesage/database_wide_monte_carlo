
from brightway2 import *
from pathlib import Path
import numpy as np
import os
import io
from math import ceil
import sqlite3
import multiprocessing as mp
import pickle
import json
from water_balancing_data import get_water_balancing_data
from water_balancing import balance_water_exchanges
from land_use_balancing_data import get_land_use_balancing_data
from land_use_balancing import balance_land_use_exchanges


def setup(project_name, ecospold_dirpath, database_name):
    assert isinstance(project_name, str)
    assert isinstance(database_name, str)
    bw2setup()
    if database_name in databases:
        print("Database {} already existed, it will be deleted".format(database_name))
        Database(database_name).delete()
        Database(database_name).deregister()
    importer = SingleOutputEcospold2Importer(
        dirpath=ecospold_dirpath,
        db_name = database_name
    )
    importer.apply_strategies()
    importer.write_database()

class direct_solving_MC(MonteCarloLCA, DirectSolvingMixin):
    """Class expanding MonteCarloLCA to include `solve_linear_system`."""
    pass


def correlated_MCs_worker(worker_id,
                          functional_units_list,
                          iterations,
                          include_inventory,
                          include_supply,
                          include_matrices,
                          balance_water,
                          balance_land_use,
                          cur,
                          result_dir
                          ):
    """Generate database-wide correlated Monte Carlo samples

    This function is a worker function. It is called from
    the `generate_samples` function, that dispatches the Monte Carlo
    work to a specified number of workers.
    """

    # Create a factice functional unit that spans all possible demands
    # Useful if some activities link to other upstream databases

    collector_functional_unit = {k: v
                                 for d in functional_units_list
                                 for k, v in d.items()
                                 }
    # Create an LCA object that spans all demands
    lca = direct_solving_MC(demand=collector_functional_unit)
    # Build technosphere and biosphere matrices and corresponding rng
    lca.load_data()

    for iteration in range(iterations):

        print("Worker {}, iteration {}".format(worker_id, iteration))
        # Sample new values for technosphere and biosphere matrices
        lca.rebuild_technosphere_matrix(lca.tech_rng.next())
        lca.rebuild_biosphere_matrix(lca.bio_rng.next())
        if balance_water:
            lca = balance_water_exchanges(lca, os.path.join(result_dir, 'common_files'))
        if balance_land_use:
            lca = balance_land_use_exchanges(lca, os.path.join(result_dir, 'common_files'))

        if include_matrices:
            cur.execute('INSERT INTO A_matrix_samples (worker, iteration, arr) values (?, ?, ?)',
                        (worker_id, iteration, lca.technosphere_matrix.tocoo().data.astype(np.float32)))
            cur.execute('INSERT INTO B_matrix_samples (worker, iteration, arr) values (?, ?, ?)',
                        (worker_id, iteration, lca.biosphere_matrix.tocoo().data.astype(np.float32)))

        if any([include_inventory, include_supply]):
            # Factorize technosphere matrix, creating a solver
            lca.decompose_technosphere()
            # For all activities, calculate and save
            # supply and inventory vectors

            for fu in functional_units_list:
                actKey = str(list(fu.keys())[0][1])
                lca.build_demand_array(fu)
                lca.supply_array = lca.solve_linear_system()
                if include_supply:
                    cur.execute('INSERT INTO supply_samples (activity, worker, iteration, arr) values (?, ?, ?, ?)',
                                (actKey, worker_id, iteration, np.array(lca.supply_array, dtype=np.float32)))
                # Inventory
                if include_inventory:
                    lca.inventory = lca.biosphere_matrix * lca.supply_array
                    cur.execute('INSERT INTO lci_samples (activity, worker, iteration, arr) values (?, ?, ?, ?)',
                                (actKey, worker_id, iteration, np.array(lca.inventory, dtype=np.float32)))

    print(
        "Worker {} finished {} iterations".format(
            worker_id,
            iterations
        )
    )

def get_useful_info(collector_functional_unit, results_dirpath, activities, database_name, project_name, balance_water,
                    balance_land_use):
    """Collect and save job-level data"""

    # Generate sacrificial LCA whose attributes will be saved
    sacrificial_lca = LCA(collector_functional_unit)
    sacrificial_lca.lci()

    # Make folder to contain extracted information
    common_dir = os.path.join(results_dirpath, 'common_files')
    if not os.path.isdir(common_dir):
        os.makedirs(common_dir)

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
        pickle.dump({v: k for k, v in mapping.items()}, f)

    fp = os.path.join(common_dir, 'tech_row_indices')
    np.save(fp, sacrificial_lca.technosphere_matrix.tocoo().row)

    fp = os.path.join(common_dir, 'tech_col_indices')
    np.save(fp, sacrificial_lca.technosphere_matrix.tocoo().col)

    fp = os.path.join(common_dir, 'bio_row_indices')
    np.save(fp, sacrificial_lca.biosphere_matrix.tocoo().row)

    fp = os.path.join(common_dir, 'bio_col_indices')
    np.save(fp, sacrificial_lca.biosphere_matrix.tocoo().col)

    if balance_water:
        get_water_balancing_data(results_dirpath, activities, database_name, project_name, sacrificial_lca)
    if balance_land_use:
        get_land_use_balancing_data(results_dirpath, activities, database_name, project_name, sacrificial_lca)

    return None

def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]


def concat_vectors_worker(activity_list, output_type, cur):
    """Worker to concatenate and save samples for a given job"""

    if output_type=="lci":
        output_folder = results_dirpath / "lci"
    elif output_type=='supply':
        output_folder = results_dirpath / "supply"
    elif output_type in ['A_matrix', 'B_matrix']:
        output_folder = results_dirpath / "matrices"
    else:
        raise ValueError("output type not valid")
    output_folder.mkdir(exist_ok=True)
    if output_type not in ['A_matrix', 'B_matrix']:
        for act in activity_list:
            if act + '.npy' in os.listdir(output_folder):
                pass

        else:
            cur.execute("SELECT arr "
                        "FROM {} "
                        "WHERE activity = ? "
                        "ORDER BY worker, iteration".format(output_type),
                        (act,)
                        )

            files = list(cur.fetchall())
            arr = np.concatenate(files, axis=0).T
            np.save(file=os.path.join(output_folder, act), arr=arr)
    else: # Matrix
        if output_type + '.npy' in os.listdir(output_folder):
            pass
        else:
            cur.execute("SELECT arr "
                        "FROM {} "
                        "ORDER BY worker, iteration".format(output_type)
                        )

            files = list(cur.fetchall())
            arr = np.concatenate(files, axis=0).T
            np.save(file=os.path.join(output_folder, output_type), arr=arr)
    return None

def adapt_array(arr):
    out = io.BytesIO()
    np.save(out, arr)
    out.seek(0)
    return sqlite3.Binary(out.read())


def convert_array(text):
    out = io.BytesIO(text)
    out.seek(0)
    return np.load(out)

if __name__ == "__main__":
    balance_land_use = True
    balance_water = True
    project_name = "ei_34_dbwide"
    projects.set_current(project_name)
    #ecospold_dirpath = Path(".") / "ecospolds"
    ecospold_dirpath = Path(r"C:\mypy\data\ecoinvent_spolds\ecoinvent34\datasets")
    results_dirpath = Path(".") / "results"
    database_name = "ei34"
    cpus = 4
    iterations = 10
    include_inventory=True
    include_supply=False
    include_matrices=False

    sqlite3.register_adapter(np.ndarray, adapt_array)
    sqlite3.register_converter("array", convert_array)
    conn = sqlite3.connect('db_wide_mcs.sqlite', detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    cur.execute('CREATE TABLE lci (activity VARCHAR, worker INTEGER, iteration INTEGER, arr array)')
    cur.execute('CREATE TABLE supply (activity VARCHAR, worker INTEGER, iteration INTEGER, arr array)')
    cur.execute('CREATE TABLE A_matrix (worker INTEGER, iteration INTEGER, arr array)')
    cur.execute('CREATE TABLE B_matrix (worker INTEGER, iteration INTEGER, arr array)')
    conn.commit()
    __spec__ = None

    setup(project_name, ecospold_dirpath, database_name)
    db = Database(database_name)
    collector_functional_unit = {act.key: act['production amount'] for act in db}
    activities = [k[1] for k in collector_functional_unit.keys()][0:10]
    get_useful_info(collector_functional_unit, results_dirpath, activities, database_name, project_name, balance_water,
                    balance_land_use)
    # Calculate number of iterations per worker.
    it_per_worker = [iterations//cpus for _ in range(cpus)]
    for _ in range(iterations-cpus*(iterations//cpus)):
        it_per_worker[_]+=1
    workers = []

    for worker_id in range(cpus):
        correlated_MCs_worker(worker_id=worker_id, functional_units_list=[{act:amount} for act, amount in collector_functional_unit.items()],
                              iterations=it_per_worker[worker_id], include_inventory=include_inventory, include_supply=include_supply,
                              include_matrices=include_matrices, balance_water=balance_water,
                              balance_land_use=balance_land_use, cur=cur, result_dir=results_dirpath)
        child = mp.Process(target=correlated_MCs_worker,
                           args=(
                               worker_id,
                               [{act:amount} for act, amount in collector_functional_unit.items()],
                               it_per_worker[worker_id],
                               include_inventory,
                               include_supply,
                               include_matrices,
                               balance_water,
                               balance_land_use,
                               cur,
                               results_dirpath
                           )
                           )
        workers.append(child)
        child.start()
    for c in workers:
        c.join()

    # Aggregate
    with open(results_dirpath / 'common_files' / 'activity_UUIDs.json', 'r') as file:
        act_list = json.load(file)
    activity_sublists = chunks(act_list, ceil(len(act_list) / cpus))
    if include_inventory:
        workers = []
        for s in activity_sublists:
            j = mp.Process(target=concat_vectors_worker,
                           args=(s,
                                 'lci',
                                 cur
                                 )
                           )
            workers.append(j)
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    if include_matrices:
        for matrix in ['A_matrix', 'B_matrix']:
            j = mp.Process(target=concat_vectors_worker,
                           args=(None,
                                 matrix,
                                 cur
                                 )
                           )
            workers.append(j)
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    if include_matrices:
        workers = []
        for s in activity_sublists:
            j = mp.Process(target=concat_vectors_worker,
                           args=(s,
                                 'lci',
                                 cur
                                 )
                           )
            workers.append(j)
    for w in workers:
        w.start()
    for w in workers:
        w.join()

