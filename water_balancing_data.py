from brightway2 import *
import os
import pickle
import pyprind
from collections import defaultdict
from bw2data.backends.peewee.schema import ExchangeDataset
from techno_water_exchange_names import intermediate_exchange_names

def get_water_balancing_data(job_dir, activities, database_name, project_name,
                             sacrificial_lca):
    """Collect and save job-level data for water balancing"""
    print("getting data to balance water exchanges")
    projects.set_current(project_name)

    # Make folder to contain extracted information
    common_dir = os.path.join(job_dir, 'common_files')
    assert os.path.isdir(common_dir), "common_file directory missing"
    water_dir = os.path.join(common_dir, "water_info")
    os.makedirs(water_dir)

    # Extract and save data on individual water exchanges
    print("extract data on exchanges")
    techno_keys_product, techno_keys_waste,\
    ef_input_keys, ef_output_keys, \
    unit_scaling_techno_product, unit_scaling_techno_waste = \
        get_info_on_exchanges(database_name, water_dir)

    print("assign water balancing strategies")
    strategies, strategy_lists = assign_strategies(
        database_name,
        techno_keys_product, techno_keys_waste,
        ef_input_keys, ef_output_keys, water_dir
    )
    print("generating data for default strategy")
    generate_default_strategy_data(
        strategy_lists,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
        unit_scaling_techno_product, unit_scaling_techno_waste,
        sacrificial_lca, water_dir
    )
    print("generating data for inverse strategy")
    generate_inverse_strategy_data(
        strategy_lists,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
        unit_scaling_techno_product, unit_scaling_techno_waste,
        sacrificial_lca, water_dir
    )

    print("generating data for set_static strategy")
    generate_set_static_data(
        strategy_lists,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
        sacrificial_lca,
        water_dir
    )


def generate_set_static_data(
        strategy_lists,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
        sacrificial_lca,
        water_dir):
    set_static_data = {}
    for act in strategy_lists['set_static']:
        set_static_data[act] = generate_set_static_data_single_act(
        sacrificial_lca, act,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
    )
    with open(os.path.join(water_dir, "set_static_data.pickle"), "wb") as f:
        pickle.dump(set_static_data, f)


def generate_set_static_data_single_act(
        lca, act_key,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product
):
    """Identify rows that need to be considered in balancing"""
    act = get_activity(act_key)
    col = lca.activity_dict[act.key]

    ef = [
        exc.input.key for exc in act.biosphere()
        if exc.input in ef_input_keys + ef_output_keys
    ]
    techno = [
        exc.input.key for exc in act.technosphere()
        if exc.input in techno_keys_waste + techno_keys_product
    ]
    production = [
        exc.input.key for exc in act.production()
        if exc.input in techno_keys_waste + techno_keys_product
    ]
    bio_rows = [lca.biosphere_dict[k] for k in ef]
    techno_rows = [lca.product_dict[k] for k in techno + production]
    return {
        'bio_rows': ef,
        'bio_values': [lca.biosphere_matrix[r, col] for r in bio_rows],
        'techno_rows': techno + production,
        'techno_values': [lca.technosphere_matrix[r, col] for r in techno_rows]
    }

def generate_inverse_strategy_data(
        strategy_lists,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
        unit_scaling_techno_product, unit_scaling_techno_waste,
        sacrificial_lca, water_dir
):
    initial_ratios_inverse = {}
    print("Calculate initial in/out ratios for inverse strategy activities")
    for act in pyprind.prog_bar(strategy_lists['inverse']):
        initial_ratios_inverse[act] = 1/initial_in_over_out(
            act,
            ef_input_keys, ef_output_keys,
            techno_keys_waste, techno_keys_product,
            unit_scaling_techno_product, unit_scaling_techno_waste
        )

    print("getting row incides for inverse strategy")
    rows_of_interest_inverse = {}
    for act in pyprind.prog_bar(strategy_lists['inverse']):
        rows_of_interest_inverse[act] = identify_rows_of_interest_inverse(
        sacrificial_lca, act,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product
        )

    with open(os.path.join(water_dir, "initial_ratios_inverse.pickle"), "wb") as f:
        pickle.dump(initial_ratios_inverse, f)
    with open(os.path.join(water_dir, "rows_of_interest_inverse.pickle"), "wb") as f:
        pickle.dump(rows_of_interest_inverse, f)


def identify_rows_of_interest_inverse(
        lca, act_key,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product):

    """Identify rows that need to be considered in balancing"""
    act = get_activity(act_key)
    ef_out_exc_to_balance = [
        exc.input.key for exc in act.biosphere()
        if exc.input in ef_output_keys
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
    ]
    ef_out_exc_static = [
        exc.input.key for exc in act.biosphere()
        if exc.input in ef_output_keys
           and exc['uncertainty type'] == 0
           and exc['amount'] != 0
    ]
    techno_out_product_to_balance = [
        exc.input.key for exc in act.production()
        if exc.input in techno_keys_product
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
           and exc.input.key != act.key
    ]
    techno_out_product_static = [
        exc.input.key for exc in act.production()
        if exc.input in techno_keys_product
           and exc['uncertainty type'] == 0
           and exc.input.key != act.key
           and exc['amount'] != 0
    ]
    techno_out_waste_to_balance = [
        exc.input.key for exc in act.technosphere()
        if exc.input in techno_keys_waste
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
    ]
    techno_out_waste_static = [
        exc.input.key for exc in act.technosphere()
        if exc.input in techno_keys_waste
           and exc['uncertainty type'] == 0
           and exc['amount'] != 0
    ]

    ef_in_exc = [exc.input.key for exc in act.biosphere() if exc.input in ef_input_keys]
    techno_in_product = [exc.input.key for exc in act.technosphere() if exc.input in techno_keys_product]
    techno_in_waste = [exc.input.key for exc in act.production() if exc.input in techno_keys_waste]

    return {
        "ef_out_to_balance": ef_out_exc_to_balance,
        "ef_out_static": ef_out_exc_static,
        "techno_out_product_to_balance": techno_out_product_to_balance,
        "techno_out_product_static": techno_out_product_static,
        "techno_out_waste_to_balance": techno_out_waste_to_balance,
        "techno_out_waste_static": techno_out_waste_static,
        "ef_in": ef_in_exc,
        "techno_in_product": techno_in_product,
        "techno_in_waste": techno_in_waste,
    }



def generate_default_strategy_data(
        strategy_lists,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
        unit_scaling_techno_product, unit_scaling_techno_waste,
        sacrificial_lca, water_dir
):
    initial_ratios_default = {}
    print("Calculate initial in/out ratios for default strategy activities")
    for act in pyprind.prog_bar(strategy_lists['default']):
        initial_ratios_default[act] = initial_in_over_out(
            act,
            ef_input_keys, ef_output_keys,
            techno_keys_waste, techno_keys_product,
            unit_scaling_techno_product, unit_scaling_techno_waste
        )
    rows_of_interest_default = {}

    print("getting rows of interest for default strategy")
    for act in pyprind.prog_bar(strategy_lists['default']):
        rows_of_interest_default[act] = identify_rows_of_interest_default(
        sacrificial_lca, act,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product)

    with open(os.path.join(water_dir, "initial_ratios_default.pickle"), "wb") as f:
        pickle.dump(initial_ratios_default, f)
    with open(os.path.join(water_dir, "rows_of_interest_default.pickle"), "wb") as f:
        pickle.dump(rows_of_interest_default, f)

def initial_in_over_out(
        bw_act_key,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
        unit_scaling_techno_product, unit_scaling_techno_waste
):
        """ Return original ratio of sum of water exchanges"""
        bw_act = get_activity(bw_act_key)
        in_sum = 0
        out_sum = 0
        for exc in bw_act.technosphere():
            if exc.input in techno_keys_product:
                in_sum += exc['amount'] * unit_scaling_techno_product[exc.input]
            if exc.input in techno_keys_waste:
                out_sum += -exc['amount'] * unit_scaling_techno_waste[exc.input]
        for exc in bw_act.biosphere():
            if exc.input in ef_input_keys:
                in_sum += exc['amount'] * 1000
            if exc.input in ef_output_keys:
                out_sum += exc['amount'] * 1000
        for exc in bw_act.production():
            if exc.input in techno_keys_product:
                out_sum += exc['amount'] * unit_scaling_techno_product[exc.input]
            if exc.input in techno_keys_waste:
                in_sum += -exc['amount'] * unit_scaling_techno_waste[exc.input]
        return in_sum / out_sum


def identify_rows_of_interest_default(
        lca, act_key,
        ef_input_keys, ef_output_keys,
        techno_keys_waste, techno_keys_product,
):
    """Identify rows that need to be considered in balancing"""
    act = get_activity(act_key)
    ef_in_exc_to_balance = [
        exc.input.key for exc in act.biosphere()
        if exc.input in ef_input_keys
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
    ]
    ef_in_exc_static = [
        exc.input.key for exc in act.biosphere()
        if exc.input in ef_input_keys
           and exc['uncertainty type'] == 0
           and exc['amount'] != 0
    ]
    techno_in_product_to_balance = [
        exc.input.key for exc in act.technosphere()
        if exc.input in techno_keys_product
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
           and exc.input.key != act.key
    ]
    techno_in_product_static = [
        exc.input.key for exc in act.technosphere()
        if exc.input in techno_keys_product
           and exc['uncertainty type'] == 0
           and exc.input.key != act.key
           and exc['amount'] != 0
    ]
    techno_in_waste_to_balance = [
        exc.input.key for exc in act.production()
        if exc.input in techno_keys_waste
           and exc['uncertainty type'] != 0
           and exc['amount'] != 0
    ]
    techno_in_waste_static = [
        exc.input.key for exc in act.production()
        if exc.input in techno_keys_waste
           and exc['uncertainty type'] == 0
           and exc['amount'] != 0
    ]

    ef_out_exc = [exc.input.key for exc in act.biosphere() if exc.input in ef_output_keys]
    techno_out_product = [exc.input.key for exc in act.production() if exc.input in techno_keys_product]
    techno_out_waste = [exc.input.key for exc in act.technosphere() if
                        exc.input in techno_keys_waste and exc.input.key != act.key]

    return {
        "ef_in_to_balance": ef_in_exc_to_balance,
        "ef_in_static": ef_in_exc_static,
        "techno_in_product_to_balance": techno_in_product_to_balance,
        "techno_in_product_static": techno_in_product_static,
        "techno_in_waste_to_balance": techno_in_waste_to_balance,
        "techno_in_waste_static": techno_in_waste_static,
        "ef_out": ef_out_exc,
        "techno_out_product": techno_out_product,
        "techno_out_waste": techno_out_waste,
    }

def assign_strategies(database_name,
        techno_keys_product, techno_keys_waste,
        ef_input_keys, ef_output_keys, water_dir
):
    strategies = {}
    for act in pyprind.prog_bar(Database(database_name)):
        strategies[act.key] = activity_strategy_triage(
            act,
            techno_keys_product, techno_keys_waste,
            ef_input_keys, ef_output_keys)
    strategy_lists = defaultdict(list)
    for v, k in strategies.items():
        strategy_lists[k].append(v)

    file = os.path.join(water_dir, 'strategies.pickle')
    with open(file, "wb") as f:
        pickle.dump(strategies, f)

    file = os.path.join(water_dir, 'strategy_lists.pickle')
    with open(file, "wb") as f:
        pickle.dump(strategy_lists, f)

    return strategies, strategy_lists




def get_info_on_exchanges(database_name, water_dir):
    """Extract and format data on water exchanges"""

    # Get list of water bio exchanges
    input_bio_exchanges, output_bio_exchanges = get_bio_exchanges(database_name)

    # Get list of water techno exchanges
    # Actual names were imported above
    activities_with_water_reference_flows = [
        act for act in Database(database_name)
        if act['reference product'] in intermediate_exchange_names
    ]
    ww_acts = [
        act for act in activities_with_water_reference_flows
        if act['production amount']<0
    ]
    product_acts = [
        act for act in activities_with_water_reference_flows
        if act['production amount'] > 0
    ]

    ef_input_keys = [ef.key for ef in input_bio_exchanges]
    ef_output_keys = [ef.key for ef in output_bio_exchanges]
    techno_keys_waste = [techno.key for techno in ww_acts]
    techno_keys_product = [techno.key for techno in product_acts]
    all_water_keys = ef_input_keys + ef_output_keys + techno_keys_waste + techno_keys_product

    unit_scaling_techno_product = {}
    for k in techno_keys_product:
        unit_scaling_techno_product[k] = 1 if get_activity(k)['unit'] == "kilogram" else 1000

    unit_scaling_techno_waste = {}
    for k in techno_keys_waste:
        unit_scaling_techno_waste[k] = 1 if get_activity(k)['unit'] == "kilogram" else 1000

    # Save data for reuse

    file = os.path.join(water_dir, 'ef_input_keys.pickle')
    with open(file, "wb") as f:
        pickle.dump(ef_input_keys, f)

    file = os.path.join(water_dir, 'ef_output_keys.pickle')
    with open(file, "wb") as f:
        pickle.dump(ef_output_keys, f)

    file = os.path.join(water_dir, 'techno_keys_waste.pickle')
    with open(file, "wb") as f:
        pickle.dump(techno_keys_waste, f)

    file = os.path.join(water_dir, 'techno_keys_product.pickle')
    with open(file, "wb") as f:
        pickle.dump(techno_keys_product, f)

    file = os.path.join(water_dir, 'all_water_keys.pickle')
    with open(file, "wb") as f:
        pickle.dump(all_water_keys, f)

    file = os.path.join(water_dir, 'unit_scaling_techno_product.pickle')
    with open(file, "wb") as f:
        pickle.dump(unit_scaling_techno_product, f)

    file = os.path.join(water_dir, 'unit_scaling_techno_waste.pickle')
    with open(file, "wb") as f:
        pickle.dump(unit_scaling_techno_waste, f)

    return techno_keys_product, techno_keys_waste, \
           ef_input_keys, ef_output_keys, \
           unit_scaling_techno_product, unit_scaling_techno_waste


def get_bio_exchanges(database_name):
    """ Identify water biosphere exchanges to consider in balancing"""
    elementary_flow_candidates = [ef for ef in Database('biosphere3') if 'Water' in ef['name']]
    elementary_flows = [
        ef for ef in elementary_flow_candidates
        if check_bio_exc_used_by_database(ef.key, database_name)
    ]
    input_bio_exchanges = [
        ef for ef in elementary_flows
        if ef['categories'][0] == 'natural resource'
    ]
    output_bio_exchanges = [
        ef for ef in elementary_flows
        if ef['categories'][0] != 'natural resource'
    ]
    return input_bio_exchanges, output_bio_exchanges


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
        techno_keys_product, techno_keys_waste,
        ef_input_keys, ef_output_keys
):
    """ Determine what strategy to apply"""
    exchanges = [exc for exc in act.exchanges()]
    water_exc_product_inputs = [
        exc for exc in exchanges
        if exc['input'] in techno_keys_product
           and exc['type'] == "technosphere"
    ]
    water_exc_product_outputs = [
        exc for exc in exchanges
        if exc['input'] in techno_keys_product
           and exc['type'] == "production"
    ]
    water_exc_waste_intermediary = [
        exc for exc in exchanges
        if exc['input'] in techno_keys_waste
           and exc['type'] == "technosphere"
    ]
    water_exc_waste_product = [
        exc for exc in exchanges
        if exc['input'] in techno_keys_waste
           and exc['type'] == "production"
    ]
    water_ef_in = [
        exc for exc in exchanges
        if exc['input'] in ef_input_keys
    ]
    water_ef_out = [
        exc for exc in exchanges
        if exc['input'] in ef_output_keys
    ]
    # Determine whether there are water exchange inputs and outputs
    water_inputs_present = any(
        [
            water_exc_product_inputs,
            water_exc_waste_product,
            water_ef_in
        ]
    )
    water_outputs_present = any(
        [
            water_exc_product_outputs,
            water_exc_waste_intermediary,
            water_ef_out
        ]
    )

    if not any(
            [
                water_inputs_present,
                water_outputs_present
            ]
    ):
        return "skip"

    all_exc_out = water_exc_product_outputs + water_exc_waste_intermediary + water_ef_out
    all_exc_in = water_exc_product_inputs + water_exc_waste_product + water_ef_in

    exc_wrong_sign = []
    for exc in water_exc_product_outputs + water_ef_out + water_exc_product_inputs + water_ef_in:
        if exc['amount'] < 0:
            exc_wrong_sign.append(exc)
    for exc in water_exc_waste_intermediary + water_exc_waste_product:
        if exc['amount'] > 0:
            exc_wrong_sign.append(exc)
    if len(exc_wrong_sign) > 0:
        print(act['code'], act['name'])
        for exc in exc_wrong_sign:
            print("wrong sign: ", exc)

    non_zero_in = [exc for exc in all_exc_in if exc['amount'] != 0]
    non_zero_out = [exc for exc in all_exc_out if exc['amount'] != 0]

    if len(non_zero_in) + len(non_zero_out) == 0:
        return "skip"

    if len(all_exc_out) + len(all_exc_in) == 1:
        return "skip"

    if not all_exc_out:
        return "skip"

    if not all_exc_in:
        return "skip"

    if len(non_zero_in) == 0:
        return "skip"

    if len(non_zero_out) == 0:
        return "skip"

    exc_with_uncertainty_inputs = [exc for exc in all_exc_in if exc['uncertainty type'] != 0]
    exc_with_uncertainty_outputs = [exc for exc in all_exc_out if exc['uncertainty type'] != 0]

    if len(exc_with_uncertainty_inputs + exc_with_uncertainty_outputs) == 0:
        return "skip"

    if len(exc_with_uncertainty_inputs + exc_with_uncertainty_outputs) == 1:
        return "set_static"

    if len(exc_with_uncertainty_inputs) == 0:
        return "inverse"
    if len(exc_with_uncertainty_outputs) == 0:
        return "default"

    if len(exc_with_uncertainty_inputs + exc_with_uncertainty_outputs) == len(all_exc_out) + len(all_exc_in):
        return "default"
    else:
        return "default"