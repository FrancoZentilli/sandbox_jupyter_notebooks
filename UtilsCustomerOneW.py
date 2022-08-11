#!/usr/bin/env python
# coding: utf-8

# # Packages

import pandas as pd
from pathlib import Path
import yaml
import os

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

# # Consolidate Version

def get_globals_paths(repo_path, realm, base_path_name="base_path"):
    globals_paths = {}
    realm_path = repo_path / "realm" / realm
    with open(realm_path / "conf/globals/dev.yaml") as file:
        yaml_file = yaml.load(file, Loader=yaml.FullLoader)
    base_path = yaml_file[base_path_name]
    var_sffx = "${.%s}" % base_path_name
    for k in yaml_file:
        globals_paths[k] = yaml_file[k].replace(var_sffx, base_path)
    
    return globals_paths

def get_data_paths_from_catalog(globals_paths, catalog_path):
    """
        catalog_path: path.to.catalog.yaml
    """
    data_paths = {}
    catalog_dir_path = catalog_path.parent
    with open(catalog_path) as file:
        yaml_file = yaml.load(file, Loader=yaml.FullLoader)
        
    for k0, its0 in yaml_file.items():
        for k1, its1 in its0.items():
            if k1 == "filepath":
                its1_splits = its1.split("/")
                sffx = its1_splits[0]
                for g in globals_paths:
                    if g in sffx:
                        data_paths[k0.split("@")[0]] = "{}/{}".format(globals_paths[g],"/".join(its1_splits[1:]))
    
    return data_paths

def extensive_describe(table):
    print("Display")
    display(table.head())
    for t in ("object", None):
        print("Type: {}".format("numeric" if t is None else t))
        desc = table.describe(include=t)
        desc.loc['dtype'] = table.dtypes
        desc.loc['size'] = len(table)
        desc.loc['nan %'] = table.isnull().mean()
        display(desc)
    return table

def get_table_from_catalog(catalog_path, repo_path=None, table_name=None, realm=None, summary=True):
    realms = ["rb", "cr"]
    table = pd.DataFrame()
    
    if realm is None:
        for r in realms:
            if r in catalog_path.parts:
                realm=r
    realm_path = repo_path / "realm/{}".format(realm)
    globals_paths = get_globals_paths(repo_path=repo_path, realm=realm)
    data_paths = get_data_paths_from_catalog(globals_paths=globals_paths, catalog_path=catalog_path)
    data_path = realm_path / data_paths[table_name]
    table = pd.read_parquet(data_path)
    
    if summary:
        extensive_describe(table)
        
    
    return table


