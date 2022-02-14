import os
import yaml

def plot_config(config_file = 'config/plot.yaml'):
    with open(config_file) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
        configs = configs['configs']
    return configs
