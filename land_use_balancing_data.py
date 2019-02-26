from brightway2 import *
import os
import pickle
import pyprind
from collections import defaultdict
from bw2data.backends.peewee.schema import ExchangeDataset


def get_land_use_balancing_data(
        job_dir,
        activities,
        database_name,
        project_name,
        sacrificial_lca):
    """Collect and save job-level data for land use balancing"""
    print("getting data to balance land use exchanges")
    projects.set_current(project_name)

    # Make folder to contain extracted information
    common_dir = os.path.join(job_dir, 'common_files')
    assert os.path.isdir(common_dir), "common_file directory missing"
    land_use_dir = os.path.join(common_dir, "land_use_info")
    os.makedirs(land_use_dir)

    # Extract and save data on individual land transformation exchanges
    print("extract data on exchanges")
    transformation_from, transformation_to = get_info_on_exchanges(database_name, land_use_dir)

    print("assign balancing strategies")
    strategies, strategy_lists = assign_strategies(
        database_name,
        transformation_from, transformation_to,
        land_use_dir
    )
    print("generating data for default strategy")
    generate_default_strategy_data(
        strategy_lists,
        transformation_from, transformation_to,
        sacrificial_lca, land_use_dir
    )
    print("generating data for inverse strategy")
    generate_inverse_strategy_data(
        strategy_lists,
        transformation_from, transformation_to,
        sacrificial_lca, land_use_dir
    )

    print("generating data for set_static strategy")
    generate_set_static_data(
        strategy_lists,
        transformation_from, transformation_to,
        sacrificial_lca,
        land_use_dir
    )


def generate_set_static_data(
        strategy_lists,
        transformation_from, transformation_to,
        sacrificial_lca,
        land_use_dir):
    set_static_data = {}
    for act in strategy_lists['set_static']:
        set_static_data[act] = generate_set_static_data_single_act(
        sacrificial_lca, act,
        transformation_from, transformation_to,
    )
    with open(os.path.join(land_use_dir, "set_static_data.pickle"), "wb") as f:
        pickle.dump(set_static_data, f)


def generate_set_static_data_single_act(
        lca, act_key,
        transformation_from, transformation_to
):
    """Identify rows that need to be considered in balancing"""
    act = get_activity(act_key)
    col = lca.activity_dict[act.key]

    ef = [
        exc.input.key for exc in act.biosphere()
        if exc.input in transformation_from + transformation_to
    ]
    bio_rows = [lca.biosphere_dict[k] for k in ef]

    return {
        'bio_rows': ef,
        'bio_values': [lca.biosphere_matrix[r, col] for r in bio_rows],
    }

def generate_inverse_strategy_data(
        strategy_lists,
        transformation_from, transformation_to,
        sacrificial_lca, land_use_dir
):

    if strategy_lists['inverse']:
        initial_ratios_inverse = {}
        print("Calculate initial in/out ratios for inverse strategy activities")
        for act in pyprind.prog_bar(strategy_lists['inverse']):
            initial_ratios_inverse[act] = 1/initial_in_over_out(
                act,
                transformation_from, transformation_to,
            )

        print("getting keys for inverse strategy")
        rows_of_interest_inverse = {}
        for act in pyprind.prog_bar(strategy_lists['inverse']):
            rows_of_interest_inverse[act] = identify_rows_of_interest_inverse(
            sacrificial_lca, act,
            transformation_from, transformation_to,
            )

        with open(os.path.join(land_use_dir, "initial_ratios_inverse.pickle"), "wb") as f:
            pickle.dump(initial_ratios_inverse, f)
        with open(os.path.join(land_use_dir, "rows_of_interest_inverse.pickle"), "wb") as f:
            pickle.dump(rows_of_interest_inverse, f)


def identify_rows_of_interest_inverse(
        lca, act_key,
        transformation_from, transformation_to):

    """Identify rows that need to be considered in balancing"""
    act = get_activity(act_key)

    ef_to_to_balance = [
        exc.input.key for exc in act.biosphere()
        if exc['input'] in transformation_to
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
    ]
    ef_to_static = [
        exc.input.key for exc in act.biosphere()
        if exc['input'] in transformation_to
           and exc['uncertainty type'] == 0
           and exc['amount'] != 0
    ]

    ef_from = [
        exc.input.key for exc in act.biosphere()
        if exc['input'] in transformation_from
    ]

    return {
        "tranformation_from": ef_from,
        "tranformation_to_static": ef_to_static,
        "tranformation_to_to_balance": ef_to_to_balance
    }


def generate_default_strategy_data(
        strategy_lists,
        transformation_from, transformation_to,
        sacrificial_lca, land_use_dir
):
    if strategy_lists['default']:
        initial_ratios_default = {}
        print("Calculate initial in/out ratios for default strategy activities")
        for act in pyprind.prog_bar(strategy_lists['default']):
            initial_ratios_default[act] = initial_in_over_out(
                act,
                transformation_from, transformation_to,
            )

        rows_of_interest_default = {}
        print("getting rows of interest for default strategy")
        for act in pyprind.prog_bar(strategy_lists['default']):
            rows_of_interest_default[act] = identify_rows_of_interest_default(
            sacrificial_lca, act,
            transformation_from, transformation_to)

        with open(os.path.join(land_use_dir, "initial_ratios_default.pickle"), "wb") as f:
            pickle.dump(initial_ratios_default, f)
        with open(os.path.join(land_use_dir, "rows_of_interest_default.pickle"), "wb") as f:
            pickle.dump(rows_of_interest_default, f)

def initial_in_over_out(
        bw_act_key,
        transformation_from, transformation_to
):
        """ Return original ratio of sum of land transformation exchanges"""
        bw_act = get_activity(bw_act_key)
        from_sum = 0
        to_sum = 0
        for exc in bw_act.biosphere():
            if exc['input'] in transformation_from:
                from_sum += exc['amount']
            if exc['input'] in transformation_to:
                to_sum += exc['amount']
        return from_sum / to_sum


def identify_rows_of_interest_default(
        lca, act_key,
        transformation_from, transformation_to
):
    """Identify rows that need to be considered in balancing"""
    act = get_activity(act_key)

    ef_from_to_balance = [
        exc.input.key for exc in act.biosphere()
        if exc['input'] in transformation_from
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
    ]
    ef_from_static = [
        exc.input.key for exc in act.biosphere()
        if exc['input'] in transformation_from
           and exc['uncertainty type'] == 0
           and exc['amount'] != 0
    ]

    ef_to = [
        exc.input.key for exc in act.biosphere()
        if exc['input'] in transformation_to
    ]

    return {
        "transformation_from_to_balance": ef_from_to_balance,
        "transformation_from_static": ef_from_static,
        "transformation_to": ef_to,
    }

def assign_strategies(database_name,
                      tranformation_from, transformation_to,
                      land_use_dir):
    strategies = {}
    for act in pyprind.prog_bar(Database(database_name)):
        strategies[act.key] = activity_strategy_triage(
            act,
            tranformation_from, transformation_to)

    strategy_lists = defaultdict(list)
    for v, k in strategies.items():
        strategy_lists[k].append(v)

    file = os.path.join(land_use_dir, 'strategies.pickle')
    with open(file, "wb") as f:
        pickle.dump(strategies, f)

    file = os.path.join(land_use_dir, 'strategy_lists.pickle')
    with open(file, "wb") as f:
        pickle.dump(strategy_lists, f)

    return strategies, strategy_lists


def get_info_on_exchanges(database_name, land_use_dir):
    """Extract and format data on land use exchanges"""

    transformation_from = [
        ef.key for ef in Database('biosphere3')
        if 'Transformation, from' in ef['name']
        and check_bio_exc_used_by_database(ef.key, database_name)
    ]
    transformation_to = [
        ef.key for ef in Database('biosphere3')
        if 'Transformation, to' in ef['name']
        and check_bio_exc_used_by_database(ef.key, database_name)
    ]

    # Save data for reuse
    file = os.path.join(land_use_dir, 'transformation_from_keys.pickle')
    with open(file, "wb") as f:
        pickle.dump(transformation_from, f)

    file = os.path.join(land_use_dir, 'transformation_to_keys.pickle')
    with open(file, "wb") as f:
        pickle.dump(transformation_to, f)

    return transformation_from, transformation_to


def check_bio_exc_used_by_database(ef_key, db_name):
    """ Identify if biosphere exchanges used in database"""
    q = ExchangeDataset.select().where(ExchangeDataset.input_code==ef_key[1])
    if len(q) == 0:
        return False
    q2 = q.select().where(ExchangeDataset.output_database == db_name)
    if len(q2) == 0:
        return False
    return True


def activity_strategy_triage(
        act,
        transformation_from, transformation_to
):
    """ Determine what strategy to apply"""
    exchanges = [exc for exc in act.biosphere()]
    transformation_from_in_act = [
        exc for exc in exchanges
        if exc['input'] in transformation_from
    ]
    transformation_to_in_act = [
        exc for exc in exchanges
        if exc['input'] in transformation_to
    ]

    if not transformation_from_in_act or not transformation_to_in_act:
        return "skip"

    non_zero_from = [exc for exc in transformation_from_in_act if exc['amount'] != 0]
    non_zero_to = [exc for exc in transformation_to_in_act if exc['amount'] != 0]

    if len(non_zero_from) == 0:
        return "skip"

    if len(non_zero_to) == 0:
        return "skip"

    exc_from_uncertainty = [exc for exc in transformation_from_in_act if exc['uncertainty type'] != 0]
    exc_to_uncertainty = [exc for exc in transformation_to_in_act if exc['uncertainty type'] != 0]

    if len(exc_from_uncertainty + exc_to_uncertainty) == 0:
        return "skip"

    if len(exc_from_uncertainty + exc_to_uncertainty) == 1:
        return "set_static"

    if len(exc_from_uncertainty) == 0:
        return "inverse"

    if len(exc_to_uncertainty) == 0:
        return "default"

    else:
        return "default"