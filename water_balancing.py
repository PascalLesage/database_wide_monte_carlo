import os
import pickle
import numpy as np
import pyprind

def balance_water_exchanges(lca, common_dir):
    """ Change values in A and B matrices of LCA"""

    strategy_lists, \
    initial_ratios_default, rows_of_interest_default, \
    initial_ratios_inverse, rows_of_interest_inverse, \
    set_static_data, \
    unit_scaling_techno_product, unit_scaling_techno_waste \
        = load_water_exchange_balancing_data(common_dir)

    print("rebalancing - default strategy")
    for act in pyprind.prog_bar(strategy_lists['default']):
        lca = scale_exc_default(
            lca=lca,
            act=act,
            rows_of_interest_default=rows_of_interest_default,
            initial_ratios_default=initial_ratios_default,
            unit_scaling_techno_product=unit_scaling_techno_product,
            unit_scaling_techno_waste=unit_scaling_techno_waste
        )

    print("rebalancing - inverse strategy")
    for act in pyprind.prog_bar(strategy_lists['inverse']):
        lca = scale_exc_inverse(
            lca=lca,
            act=act,
            rows_of_interest_inverse=rows_of_interest_inverse,
            initial_ratios_inverse=initial_ratios_inverse,
            unit_scaling_techno_waste=unit_scaling_techno_waste,
            unit_scaling_techno_product=unit_scaling_techno_product)

    print("rebalancing - set_static strategy")
    for act in pyprind.prog_bar(strategy_lists['set_static']):
        lca = scale_exc_static(lca, act, set_static_data)
    return lca


def load_water_exchange_balancing_data(common_dir):
    """ Load various files required for balancing water exchanges"""

    water_dir = os.path.join(common_dir, "water_info")

    # Dict used to assign acts to different strategies
    with open(os.path.join(water_dir, "strategy_lists.pickle"), 'rb') as f:
        strategy_lists = pickle.load(f)

    # Unit conversion factors
    with open(os.path.join(water_dir, "unit_scaling_techno_product.pickle"), 'rb') as f:
        unit_scaling_techno_product = pickle.load(f)
    with open(os.path.join(water_dir, "unit_scaling_techno_waste.pickle"), 'rb') as f:
        unit_scaling_techno_waste = pickle.load(f)

    # Default strategy data
    with open(os.path.join(water_dir, "initial_ratios_default.pickle"), 'rb') as f:
        initial_ratios_default = pickle.load(f)
    with open(os.path.join(water_dir, "rows_of_interest_default.pickle"), 'rb') as f:
        rows_of_interest_default = pickle.load(f)

    # Inverse strategy data
    with open(os.path.join(water_dir, "initial_ratios_inverse.pickle"), 'rb') as f:
        initial_ratios_inverse = pickle.load(f)
    with open(os.path.join(water_dir, "rows_of_interest_inverse.pickle"), 'rb') as f:
        rows_of_interest_inverse = pickle.load(f)

    # Set_static strategy data
    with open(os.path.join(water_dir, "set_static_data.pickle"), 'rb') as f:
        set_static_data = pickle.load(f)

    return strategy_lists, \
           initial_ratios_default, rows_of_interest_default, \
           initial_ratios_inverse, rows_of_interest_inverse, \
           set_static_data, \
           unit_scaling_techno_product, unit_scaling_techno_waste


    strategy_lists, \
    initial_ratios_default, rows_of_interest_default, \
    initial_ratios_inverse, rows_of_interest_inverse, \
    set_static_data, \
    unit_scaling_techno_product, unit_scaling_techno_waste \
        = load_water_exchange_balancing_data(common_dir)


def scale_exc_default(
        lca, act,
        rows_of_interest_default, initial_ratios_default,
        unit_scaling_techno_product, unit_scaling_techno_waste):

    """ Return indices and new amounts for exchanges that need scaling for specific act with default strategy"""
    # print("In scale_exc_default, act=", act)
    # print("rows of interest, default: ", rows_of_interest_default[act])
    # print("unit_scaling_product: ", unit_scaling_techno_product)
    # print("unit_scaling_waste: ", unit_scaling_techno_waste)

    col = lca.activity_dict[act]
    rev_product_dict = {v:k for k, v in lca.product_dict.items()}

    def get_A_rows(product_keys, lca=lca):
        return [lca.product_dict[k] for k in product_keys]

    def get_B_rows(ef_keys, lca=lca):
        return [lca.biosphere_dict[k] for k in ef_keys]

    def get_values(value_type, matrix):
        assert matrix in ['A', 'B']
        if matrix == 'A':
            matrix = lca.technosphere_matrix
            get_rows = get_A_rows
        elif matrix == 'B':
            matrix = lca.biosphere_matrix
            get_rows = get_B_rows
        return np.asarray(np.squeeze((matrix[get_rows(rows_of_interest_default[act][value_type]), col]).todense()))

    ef_in_static_sampled = get_values('ef_in_static', 'B')
    ef_in_to_balance_sampled = get_values('ef_in_to_balance', 'B')
    ef_out_sampled = get_values('ef_out', 'B')
    techno_in_product_static_sampled = - get_values('techno_in_product_static', 'A')
    techno_in_product_to_balance_sampled = - get_values('techno_in_product_to_balance', 'A')
    techno_in_waste_static_sampled = - get_values('techno_in_waste_static', 'A')
    techno_in_waste_to_balance_sampled = - get_values('techno_in_waste_to_balance', 'A')
    techno_out_product_sampled = get_values('techno_out_product', 'A')
    techno_out_waste_sampled = get_values('techno_out_waste', 'A')

    techno_in_product_static_unit_conversion = np.array(
        [unit_scaling_techno_product[r]
         for r in rows_of_interest_default[act]['techno_in_product_static']
         ]
    )
    techno_in_product_to_balance_unit_conversion = np.array(
        [unit_scaling_techno_product[r]
         for r in rows_of_interest_default[act]['techno_in_product_to_balance']
         ]
    )
    techno_in_waste_static_unit_conversion = np.array(
        [unit_scaling_techno_waste[r] for r in rows_of_interest_default[act]['techno_in_waste_static']])
    techno_in_waste_to_balance_unit_conversion = np.array(
        [unit_scaling_techno_waste[r] for r in rows_of_interest_default[act]['techno_in_waste_to_balance']])
    techno_out_product_unit_conversion = np.array(
        [unit_scaling_techno_product[r] for r in rows_of_interest_default[act]['techno_out_product']])
    techno_out_waste_unit_conversion = np.array(
        [unit_scaling_techno_waste[r] for r in rows_of_interest_default[act]['techno_out_waste']])

    total_out = np.sum(ef_out_sampled * 1000) \
                + np.sum(techno_out_product_sampled * techno_out_product_unit_conversion) \
                + np.sum(techno_out_waste_sampled * techno_out_waste_unit_conversion)

    constant_in = np.sum(ef_in_static_sampled * 1000) \
                  + np.sum(techno_in_product_static_sampled * techno_in_product_static_unit_conversion) \
                  + np.sum(techno_in_waste_static_sampled * techno_in_waste_static_unit_conversion)

    variable_in = np.sum(ef_in_to_balance_sampled * 1000) \
                  + np.sum(techno_in_product_to_balance_sampled * techno_in_product_to_balance_unit_conversion) \
                  + np.sum(techno_in_waste_to_balance_sampled * techno_in_waste_to_balance_unit_conversion)

    scaling = (initial_ratios_default[act] * total_out - constant_in) / variable_in

    tech_amounts_product = scaling * -techno_in_product_to_balance_sampled
    tech_amounts_waste = scaling * -techno_in_waste_to_balance_sampled
    ef_amounts = scaling * ef_in_to_balance_sampled

    tech_rows_product = get_A_rows(rows_of_interest_default[act]['techno_in_product_to_balance'])
    tech_rows_waste = get_A_rows(rows_of_interest_default[act]['techno_in_waste_to_balance'])
    ef_rows = get_B_rows(rows_of_interest_default[act]['ef_in_to_balance'])

    if tech_amounts_product.size > 0:
        lca.technosphere_matrix[tech_rows_product, col] = tech_amounts_product.T

    if tech_amounts_waste.size > 0:
        lca.technosphere_matrix[tech_rows_waste, col] = tech_amounts_waste.T
    if ef_amounts.size > 0:
        lca.biosphere_matrix[ef_rows, col] = ef_amounts.T

    return lca

def scale_exc_inverse(
        lca, act,
        rows_of_interest_inverse, initial_ratios_inverse,
        unit_scaling_techno_product, unit_scaling_techno_waste):
    col = lca.activity_dict[act]
    rev_product_dict = {v:k for k, v in lca.product_dict.items()}

    def get_A_rows(product_keys, lca=lca):
        return [lca.product_dict[k] for k in product_keys]

    def get_B_rows(ef_keys, lca=lca):
        return [lca.biosphere_dict[k] for k in ef_keys]

    def get_values(value_type, matrix):
        assert matrix in ['A', 'B']
        if matrix=='A':
            matrix = lca.technosphere_matrix
            get_rows = get_A_rows
        elif matrix=='B':
            matrix = lca.biosphere_matrix
            get_rows = get_B_rows
        return np.asarray(np.squeeze((matrix[get_rows(rows_of_interest_inverse[act][value_type]), col]).todense()))

    ef_out_static_sampled = get_values('ef_out_static', 'B')
    ef_out_to_balance_sampled = get_values('ef_out_to_balance', 'B')
    ef_in_sampled = get_values('ef_in', 'B')
    techno_out_product_static_sampled = get_values('techno_out_product_static', 'A')

    techno_out_product_to_balance_sampled = get_values('techno_out_product_to_balance', 'A')
    techno_out_waste_static_sampled = get_values('techno_out_waste_static', 'A')
    techno_out_waste_to_balance_sampled = get_values('techno_out_waste_to_balance', 'A')
    techno_in_product_sampled = get_values('techno_in_product', 'A')
    techno_in_waste_sampled = - get_values('techno_in_waste', 'A')
    techno_out_product_static_unit_conversion = np.array(
        [unit_scaling_techno_product[rev_product_dict[r]] for r in rows_of_interest_inverse[act]['techno_out_product_static']])
    techno_out_product_to_balance_unit_conversion = np.array([unit_scaling_techno_product[rev_product_dict[r]] for r in
                                                              rows_of_interest_inverse[act]['techno_out_product_to_balance']])
    techno_out_waste_static_unit_conversion = np.array(
        [unit_scaling_techno_waste[r] for r in rows_of_interest_inverse[act]['techno_out_waste_static']])
    techno_out_waste_to_balance_unit_conversion = np.array(
        [unit_scaling_techno_waste[r] for r in rows_of_interest_inverse[act]['techno_out_waste_to_balance']])
    techno_in_product_unit_conversion = np.array(
        [unit_scaling_techno_product[r] for r in rows_of_interest_inverse[act]['techno_in_product']])
    techno_in_waste_unit_conversion = np.array(
        [unit_scaling_techno_waste[r] for r in rows_of_interest_inverse[act]['techno_in_waste']])

    total_in = np.sum(ef_in_sampled * 1000) \
               + np.sum(techno_in_product_sampled * techno_in_product_unit_conversion) \
               + np.sum(techno_in_waste_sampled * techno_in_waste_unit_conversion)

    constant_out = np.sum(ef_out_static_sampled * 1000) \
                   + np.sum(techno_out_product_static_sampled * techno_out_product_static_unit_conversion) \
                   + np.sum(techno_out_waste_static_sampled * techno_out_waste_static_unit_conversion)

    variable_out = np.sum(ef_out_to_balance_sampled * 1000) \
                   + np.sum(techno_out_product_to_balance_sampled * techno_out_product_to_balance_unit_conversion) \
                   + np.sum(techno_out_waste_to_balance_sampled * techno_out_waste_to_balance_unit_conversion)

    scaling = (initial_ratios_inverse[act] * total_in - constant_out) / variable_out

    tech_rows_product = get_A_rows(rows_of_interest_inverse[act]['techno_out_product_to_balance'])
    tech_rows_waste = get_A_rows(rows_of_interest_inverse[act]['techno_out_waste_to_balance'])
    ef_rows = get_B_rows(rows_of_interest_inverse[act]['ef_out_to_balance'])

    tech_amounts_product = scaling * techno_out_product_to_balance_sampled
    tech_amounts_waste = - scaling * techno_out_waste_to_balance_sampled
    ef_amounts = scaling * ef_out_to_balance_sampled

    if tech_amounts_product.size > 0:
        lca.technosphere_matrix[tech_rows_product, col] = tech_amounts_product.T
    if tech_amounts_waste.size > 0:
        lca.technosphere_matrix[tech_rows_waste, col] = tech_amounts_waste.T
    if ef_amounts.size > 0:
        lca.biosphere_matrix[ef_rows, col] = ef_amounts.T

    return lca

def scale_exc_static(lca, act, set_static_data):
    data = set_static_data[act]
    col = lca.activity_dict[act]
    techno_rows = [lca.product_dict[k] for k in data['techno_rows']]
    bio_rows = [lca.biosphere_dict[k] for k in data['bio_rows']]

    lca.technosphere_matrix[techno_rows, col] = np.array(data['techno_values']).reshape(-1, 1)
    lca.biosphere_matrix[bio_rows, col] = np.array(data['bio_values']).reshape(-1, 1)
    return lca

