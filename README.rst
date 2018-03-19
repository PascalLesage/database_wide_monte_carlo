The ``database_wide_monte_carlo`` package, built on the `Brightway2 life cycle assessment framework <http://brightwaylca.org/>`_,  provides the means to generate precalculated sample arrays for a whole life cycle inventory (LCI) database.

Motivation
==========
Precalculating Monte Carlo samples for LCA matrices and associated results (e.g. aggregated LCIs, LCIA scores) can make subsequent LCA calculations much quicker. However, independently calculating Monte Carlo samples for different products in a single database ignores correlation across LCI and LCIA results. 

The ``database_wide_monte_carlo`` uses dependent sampling to generate precalculated sample arrays for a whole database. 

These can then be reused for efficient uncertainty analyses in LCA.

Structure of results
===========

- each **row** of the precalculated sample arrays represents a specific object (e.g. an input to a given activity, a cradle-to-gate result for a given elementary flow, an LCIA score). Files are generated to inform what each row refers to.  
- each **column** refers to a given Monte Carlo iteration. All arrays have the same number of columns, and each column in any of the arrays was generated with the same Monte Carlo iteration, i.e. with the same initial data in the technosphere and biosphere matrix. 
 

Types of results
==========

- Arrays with the values sampled for the technosphere matrix **A** and the elementary flow matrix **B** (called the biosphere matrix in Brightway2)
- Supply arrays **s** for unit demands of each activity (i.e. how much each unit process needs to produce to meet a unit of demand for any activity in the database)  
- Inventory arrays **g** for unit demands of each activity, functionaly equivalent to aggregated LCI datasets  
- LCIA score arrays **h** for unit demands of each activity  

Usage
===========
The steps to a typical usage are:  

- `Install <https://docs.brightwaylca.org/installation.html>`_ the `Brightway2 life cycle assessment framework <http://brightwaylca.org/>`_
- Set up the Brightway project and import the database. This can be done by running the `setup.py` file. The setup file assumes you are importing ecoSpold2 files. For another type of database, modify `setup.py` so it uses another `importer`, or simply setup the Brightway2 environment yourself. If you already have a project with the target LCI database, this step is skipped.

  ``python setup.py --project_name=my_project, --ecospold_dirpath=my_path_to_folder_with_ecospold2_files --database_name=db'``

- Dependently generate individual samples using the `sample_generation.py` file. This process can be halted and restarted, can be distributed across many computers, and relies on multiprocessing to speed up Monte carlo simulations. The results generated in one batch are found in `job` directories. 

  ``python sample_generation.py --project=my_project --database_name=db --iterations=1000 --cpus=8 --base_dir=path_to_my_folder --include_inventory=True --include_supply=True --include_matrices=True``

- Sanitize results using `clean_jobs.py`. This will delete jobs or iterations within a job that are missing information. 

  ``python clean_jobs.py --base_dir=path_to_my_folder --database_name=db --database_size=14889 --include_inventory=True --include_matrices=True --include_supply=True``
   
- Concatenate results within a job with `concatenate_within_jobs.py`. Uses multiprocessing to speed up process, but is nonetheless a **very** lengthy task.

  ``python concatenate_within_jobs.py --base_dir=path_to_my_folder --database_name=db --include_inventory=True --include_matrices=True --include_supply=True --cpus=8 --delete_raw_files=True``

- Concatenate results across jobs using `concatenate_across_jobs.py`. At this point, results can include (depending on what arguments were passed to the former functions) **A** and **B** matrix results, supply arrays **s**, cradle-to-gate inventories **g**. By design, they all have the same number of columns, and the i*th* column in any array is based on the same Monte Carlo iteration.  

  ``python across_jobs.py --base_dir=path_to_my_folder --database_name=db --project=my_project --include_inventory=True --include_matrices=True --include_supply=True --cpus=8 --delete_temps=True``

- Generate LCIA scores from the LCI results using `calculate_LCIA.py`. LCIA score arrays will be generated for all methods specified in a list saved to base_dir/database_name/results/reference_files/methods.pickle, or all methods implemented in Brightway2 if file doesn't exist.  

  ``python calculate_LCIA.py --base_dir=path_to_my_folder --database_name=db --project=my_project --cpus=8``

Warning - Time and memory!
===========
Some of the steps above (especially `sample_generation.py` and `concatenate_within_jobs.py`) can take lots of time and take up a lot of space. Depending on the database size, factor several weeks to a full month for all calculations with a typical personnal computer, and have TBs of disk available.  

To minimize time issues: 
- The more complicated tasks are `embarrassingly parallel <https://en.wikipedia.org/wiki/Embarrassingly_parallel>`_. Distribute your work on as many CPU as you can on your computer, and on multiple computers if you have some available. Note that using multiple computes will require you to move the results of `concatenate_within_jobs.py` to the computer that will eventually aggregate all the results to single arrays. 
- Make sure you use all the CPU you have at your disposal - a server cluster would be the best option.

To minimize disk space issues: 
- Delete samples and temporary files as you go along (`delete_raw_files=True` in `concatenate_within_jobs.py` and `delete_temps=True` in `concatenate_across_jobs.py`)
- Only generate the information you need. Specifically, supply arrays **s** take up lots of space, and are generally not very useful.

If you are only interested in generating correlated precalculated samples, consider using the standard `MonteCarloLCA` class in Brightway2 instead. You can seed these `MonteCarloLCA` objects, and hence conduct simulations on multiple activities in series using the same seed to ensure the same values for the **A** and **B** matrices are used for each iteration.

Examples
=========
See the examples in the Documentation section of this repo.

Contributing
=========
Don't hesitate to fork and improve this code, and to propose pull request. 

Some ideas: 

- Reduce the time used to treat millions of very tiny files by changing data storage strategy (e.g. HDF5?). 
- Create a `DatabaseWideMonteCarlo` class, and convert the functions to methods.  
We are open to suggestion.

Contributors
==========
Chris Mutel (PSI) 

Pascal Lesage (CIRAIG)

Nolwenn Kazoum
