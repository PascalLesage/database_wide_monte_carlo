import pandas as pd
import os
import pickle


def create_list_methods_from_xlsx(base_dir, database_name, xlsx_name):
    """Utility function to create method lists to pass to calculate_LCIA.py
    """
    ref_dir = os.path.join(base_dir, database_name, 'results', 'reference_files')
    
    df = pd.read_excel(os.path.join(ref_dir, xlsx_name))
    l1=df['Method']
    l2=df['Impact category (1)']
    l3=df['Impact category (2)']
    l = [(i, j, k) for i, j, k in zip(l1, l2, l3)]
    
    out_fp = os.path.join(ref_dir, xlsx_name[:-4]+'pickle')
    pickle.dump(l, open(out_fp, 'wb'))
    
    print("list saved to {}".format(out_fp))
    return None
