import os
import pickle
import numpy as np

def balance_land_use_exchanges(lca, common_dir):
    """ Change values in A and B matrices of LCA"""

    strategy_lists, \
    initial_ratios_default, rows_of_interest_default, \
    initial_ratios_inverse, rows_of_interest_inverse, \
    set_static_data \
        = load_land_use_exchange_balancing_data(common_dir)

    if strategy_lists['default']:
        print("rebalancing - default strategy")
        for act in strategy_lists['default']:
            lca = scale_exc_default(
                lca=lca,
                act=act,
                rows_of_interest_default=rows_of_interest_default,
                initial_ratios_default=initial_ratios_default,
            )

    if strategy_lists['inverse']:
        print("rebalancing - inverse strategy")
        for act in strategy_lists['inverse']:
            lca = scale_exc_inverse(
                lca=lca,
                act=act,
                rows_of_interest_inverse=rows_of_interest_inverse,
                initial_ratios_inverse=initial_ratios_inverse)


    if strategy_lists['set_static']:
        print("rebalancing - set_static strategy")
        for act in strategy_lists['set_static']:
            lca = scale_exc_static(lca, act, set_static_data)
    return lca


def load_land_use_exchange_balancing_data(common_dir):
    """ Load various files required for balancing land use exchanges"""

    land_use_dir = os.path.join(common_dir, "land_use_info")

    # Dict used to assign acts to different strategies
    with open(os.path.join(land_use_dir, "strategy_lists.pickle"), 'rb') as f:
        strategy_lists = pickle.load(f)

    # Default strategy data
    if strategy_lists['default']:
        with open(os.path.join(land_use_dir, "initial_ratios_default.pickle"), 'rb') as f:
            initial_ratios_default = pickle.load(f)
        with open(os.path.join(land_use_dir, "rows_of_interest_default.pickle"), 'rb') as f:
            rows_of_interest_default = pickle.load(f)
    else:
        initial_ratios_default=None
        rows_of_interest_default=None

    # Inverse strategy data
    if strategy_lists['inverse']:
        with open(os.path.join(land_use_dir, "initial_ratios_inverse.pickle"), 'rb') as f:
            initial_ratios_inverse = pickle.load(f)
        with open(os.path.join(land_use_dir, "rows_of_interest_inverse.pickle"), 'rb') as f:
            rows_of_interest_inverse = pickle.load(f)
    else:
        initial_ratios_inverse=None
        rows_of_interest_inverse=None
    # Set_static strategy data
    if strategy_lists['set_static']:
        with open(os.path.join(land_use_dir, "set_static_data.pickle"), 'rb') as f:
            set_static_data = pickle.load(f)
    else:
        set_static_data=None
    return strategy_lists, \
           initial_ratios_default, rows_of_interest_default, \
           initial_ratios_inverse, rows_of_interest_inverse, \
           set_static_data


    strategy_lists, \
    initial_ratios_default, rows_of_interest_default, \
    initial_ratios_inverse, rows_of_interest_inverse, \
    set_static_data, \
    unit_scaling_techno_product, unit_scaling_techno_waste \
        = load_water_exchange_balancing_data(common_dir)


def scale_exc_default(
        lca, act,
        rows_of_interest_default, initial_ratios_default):

    """ Return indices and new amounts for exchanges that need scaling for specific act with default strategy"""

    col = lca.activity_dict[act]

    def get_B_rows(ef_keys, lca=lca):
        return [lca.biosphere_dict[k] for k in ef_keys]

    def get_values(value_type):
        matrix = lca.biosphere_matrix
        get_rows = get_B_rows
        return np.asarray(np.squeeze((matrix[get_rows(rows_of_interest_default[act][value_type]), col]).todense()))

    land_from_static_sampled = get_values('transformation_from_static')
    land_from_to_balance_sampled = get_values('transformation_from_to_balance')
    land_to_sampled = get_values('transformation_to')

    scaling = (initial_ratios_default[act] * np.sum(land_to_sampled) - np.sum(land_from_static_sampled)) / np.sum(land_from_to_balance_sampled)

    balanced_amounts = scaling * land_from_to_balance_sampled
    rows = get_B_rows(rows_of_interest_default[act]['transformation_from_to_balance'])
    if balanced_amounts.size > 0:
        lca.biosphere_matrix[rows, col] = balanced_amounts.T

    return lca

def scale_exc_inverse(
        lca, act,
        rows_of_interest_inverse, initial_ratios_inverse):
    col = lca.activity_dict[act]

    def get_B_rows(ef_keys, lca=lca):
        return [lca.biosphere_dict[k] for k in ef_keys]

    def get_values(value_type):

        matrix = lca.technosphere_matrix
        get_rows = get_B_rows
        return np.asarray(np.squeeze((matrix[get_rows(rows_of_interest_inverse[act][value_type]), col]).todense()))

    land_from_sampled = get_values('transformation_from')
    land_to_to_balance_sampled = get_values('transformation_to_to_balance')
    land_to_static_sampled = get_values('transformation_to_static')

    scaling = (initial_ratios_inverse[act] * np.sum(land_from_sampled) - np.sum(land_to_static_sampled)) / np.sum(land_to_to_balance_sampled)

    balanced_amounts = scaling * land_to_to_balance_sampled
    rows = get_B_rows(rows_of_interest_inverse[act]['transformation_to_to_balance'])
    if balanced_amounts.size > 0:
        lca.biosphere_matrix[rows, col] = balanced_amounts.T

    return lca

def scale_exc_static(lca, act, set_static_data):
    data = set_static_data[act]
    col = lca.activity_dict[act]
    rows = [lca.biosphere_dict[k] for k in data['bio_rows']]

    lca.biosphere_matrix[rows, col] = np.array(data['bio_values']).reshape(-1, 1)
    return lca

