import pandas as pd
import os
import pickle


def create_list_methods_from_xls(base_dir, database_name, xls_name):
    """Utility function to create method lists to pass to calculate_LCIA.py
    """
    ref_dir = os.path.join(base_dir, database_name, 'results', 'reference_files')
    df = pd.read_excel(os.path.join(ref_dir, xls_name))
    l1=df['Method']
    l2=df['Impact category (1)']
    l3=df['Impact category (2)']
    l = [(i, j, k) for i, j, k in zip(l1, l2, l3)]
    pickle.dump(l, open(os.path.join(ref_dir, 'methods.pickle'), 'wb'))
    print("list saved to {}".format(os.path.join(ref_dir, 'methods.pickle')))
    return None
    