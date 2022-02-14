import time
import sys
from time import gmtime, strftime
import numpy as np
import pandas as pd
from matplotlib.ticker import ScalarFormatter, FuncFormatter, FormatStrFormatter
import matplotlib.pyplot as plt
import matplotlib.ticker
from pathlib import Path
from config.plot_config import plot_config
from scipy.interpolate import make_interp_spline, BSpline
import copy
def get_data():
    df = pd.read_csv(csv_file, names = columns, header = None, index_col = False)
    df = df.drop(['dimension', 'unused', 'randSeed'], axis = 1)
    test_types = plot_info['test_types']
    output = {}
    for test_type in test_types:
        testset_info = test_types[test_type]
        #if 'enabled' not in testset_info or not testset_info['enabled']:
        #    continue
        datasets = testset_info['datasets']
        method_parameters = testset_info['method_parameters'] if 'method_parameters' in testset_info else default_method_parameters_real
        testset_parameters = testset_info['testset_parameters'] if 'testset_parameters' in testset_info else default_testset_parameters

        if datasets != None:
            for dataset in datasets:
                title = dataset['title']
                if 'file' in dataset:
                    dataset_files = [dataset['file']]
                elif 'files' in dataset:
                    dataset_files = dataset['files']
                query = ['dataset_file == @dataset_files']

                for default_setting, default_value in testset_parameters.items():
                    if default_value != None:
                        query.append('`{}`=={}'.format(default_setting, default_value))

                query = '&'.join(query)
                dataset_df = df.query(query).sort_values(by = ['dataset_file'])
                method_groupby = ['method', 'num_partition', 'prob']
                method_groups = dataset_df.groupby(method_groupby)
                for method_group_id, method_group in method_groups:
                    method = method_group_id[0]
                    method_params_groupby = default_method_params_groupby
                    method_group_query = []
                    if method in method_parameters:
                        for default_setting, default_value in method_parameters[method].items():
                            if default_value != None:
                                method_group_query.append('`{}`=={}'.format(default_setting, default_value))
                    method_group_query = '&'.join(method_group_query)
                    print(method_group_query)
                    method_params_groups = method_group.query(method_group_query).groupby(method_params_groupby)
                    for method_params_group_id, method_params_group in method_params_groups:
                        dataset_groups = method_params_group.groupby(['dataset_file'])
                        for dataset, dataset_group in dataset_groups:
                            key = '{} '.format(dataset)
                            for i in range(1, len(method_groupby)):
                                key += "{}: {} | ".format(method_groupby[i], method_group_id[i])
                            for i in range(len(method_params_groupby)):
                                if method_params_group_id[i] != -1:
                                    key += "{}: {} | ".format(method_params_groupby[i], method_params_group_id[i])
                            if method not in output:
                                output[method] = {}
                            if key not in output[method]:
                                output[method][key] = []
                            describe = dataset_group['time'].describe().loc[['mean','max', 'min', 'std']]
                            time = describe['mean']
                            summary = '{} {} ({} records) | mean: {:.2f} sec | min: {:.2f} sec | max: {:.2f} sec | std: {:.2f} sec'.format(dataset, method, len(dataset_group.index), describe['mean'], describe['min'], describe['max'], describe['std'])
                            t = {'time': time, 'summary': summary}
                            output[method][key].append(t)
                            print()
                            print(test_type, summary)
                    #output += str(dataset_group)
    return output
        #exit()
def set_pandas_display_options() -> None:
    """Set pandas display options."""
    # Ref: https://stackoverflow.com/a/52432757/
    display = pd.options.display

    display.max_columns = 1000
    display.max_rows = 1000
    display.max_colwidth = 199
    display.width = 1000
    # display.precision = 2  # set as needed
def plot_prob(data):
    print('----------------- varying prob -----------------')
    allowed = ['prob_real', 'prob_synthetic']
    test_types = plot_info['test_types']
    probs = [0.2, 0.4, 0.6, 0.8]
    num_partitions = [8]
    latex(allowed, data, num_partitions, probs)
def plot_machine(data):
    print('----------------- varying machine -----------------')
    allowed = ['machine_real', 'machine_synthetic']
    test_types = plot_info['test_types']
    probs = [0.6]
    num_partitions = [4, 8, 16, 32]
    latex(allowed, data, num_partitions, probs)
def plot_instance(data):
    print('----------------- varying instance -----------------')
    allowed = ['instance_real', 'instance_synthetic']
    test_types = plot_info['test_types']
    probs = [0.6]
    num_partitions = [8]
    latex(allowed, data, num_partitions, probs)
def plot_object(data):
    print('----------------- varying object -----------------')
    allowed = ['object_real', 'object_synthetic']
    test_types = plot_info['test_types']
    probs = [0.6]
    num_partitions = [8]
    latex(allowed, data, num_partitions, probs)

def latex(allowed, data, num_partitions, probs):
    test_types = plot_info['test_types']
    nicknames = {
    'PSky' : 'ProbSky',
    'PSky-No-Grid': 'PSky-S',
    'PSky-No-Pivot': 'PSky-R',
    'PSky-No-DIP': 'PSky-D',
    'PSky-Base': 'PSky-V',
    'PSQPFMR': 'QPFMR',
    'PSBRFMR': 'BRFMR',
    }
    for test_type in test_types:
        if test_type not in allowed:
            continue
        testset_info = test_types[test_type]
        #if 'enabled' not in testset_info or not testset_info['enabled']:
        #    continue
        datasets = testset_info['datasets']
        method_parameters = testset_info['method_parameters'] if 'method_parameters' in testset_info else default_method_parameters_real
        testset_parameters = testset_info['testset_parameters'] if 'testset_parameters' in testset_info else default_testset_parameters
        methods = method_parameters.keys()
        o = {}
        if datasets != None:
            for dataset in datasets:
                title = dataset['title']
                print(title)
                if 'file' in dataset:
                    dataset_files = [dataset['file']]
                elif 'files' in dataset:
                    dataset_files = dataset['files']
                for method in methods:
                    if method not in data:
                        continue
                    if method in method_parameters:
                        method_group_query = ''
                        for default_setting, default_value in method_parameters[method].items():
                            if default_value != None and default_value != -1:
                                method_group_query += '{}: {} | '.format(default_setting, default_value)
                        output = []
                        for dataset in dataset_files:
                            for num_partition in num_partitions:
                                for prob in probs:
                                    key = f'{dataset} num_partition: {num_partition} | prob: {prob} |'
                                    final_key = f'{key} {method_group_query}'
                                    if final_key not in data[method]:
                                        output.append(-1)
                                    else:
                                        output.append(round(data[method][final_key][0]['time'], 1))
                        if method not in o:
                            o[method] = []
                        o[method] += output
                    max_length = 0

            for method, values in o.items():
              if len(values) > max_length:
                  max_length = len(values)

            temp = {}
            methods = list(o.keys())
            min_time = np.full(max_length, np.inf)
            min_time_index = np.full(max_length, np.inf)
            time = np.full((len(methods), max_length), -1)
            for i in range(max_length):
              for j in range(len(methods)):
                  method = methods[j]
                  values = o[method]
                  runtime = values[i]
                  time[j][i] = runtime
                  if runtime < min_time[i] and runtime != -1:
                      min_time[i] = runtime
                      min_time_index[i] = j

            for i in range(len(methods)):
              method = methods[i]
              output = nicknames[method] + ' '
              for j in range(max_length):
                  values = o[method]
                  runtime = values[j]
                  if runtime == -1:
                      output += '& '
                  else:
                      if min_time_index[j] == i:
                          output += '& \\textbf{' + f'{runtime:.1f}' + '} '
                      else:
                          if runtime >= 10000:
                              output += '& - '
                          else:
                              output += f'& {runtime:.1f} '
              output += '\\\\'
              print(output)
def test():
    df = pd.read_csv(csv_file, names = columns, header = None, index_col = False)
    df = df.drop(['dimension', 'unused', 'randSeed'], axis = 1)
    test_methods = ['PSky']
    test_types = plot_info['test_types']
    for test_type in ['prob_real', 'prob_synthetic']:
        testset_info = test_types[test_type]
        datasets = testset_info['datasets']
        method_parameters = testset_info['method_parameters'] if 'method_parameters' in testset_info else default_method_parameters_real
        testset_parameters = default_testset_parameters
        methods = method_parameters.keys()
        o = {}

        for dataset in datasets:
            title = dataset['title']
            print(title)
            if 'file' in dataset:
                dataset_files = [dataset['file']]
            elif 'files' in dataset:
                dataset_files = dataset['files']
            for method in methods:
                if method not in test_methods:
                    continue

                for dataset in dataset_files:
                    query = [f'`dataset_file`=="{dataset}"&`method`=="{method}"']
                    m = []

                    for default_setting, default_value in testset_parameters.items():
                        if default_value != None:
                            query.append('`{}`=={}'.format(default_setting, default_value))

                    query = '&'.join(query)
                    dataset_df = df.query(query)
                    default_settings = {}
                    for name, default_value in method_parameters[method].items():
                        default_settings[name] = default_value
                    query_testing = []
                    for name, default_value in default_settings.items():
                        pre_query = []
                        test_settings = copy.deepcopy(default_settings)
                        for test_name, test_value in test_settings.items():
                            if test_name != name:
                                pre_query.append('`{}`=={}'.format(test_name, test_value))
                        pre_query = '&'.join(pre_query)
                        test_values = dataset_df.query(pre_query)[name].unique()
                        if len(test_values) > 1:
                            print()
                            for value in test_values:
                                query = f'{pre_query}&`{name}`=={value}'
                                test_df = dataset_df.query(query)
                                time = test_df['time'].describe().loc[['mean']]['mean']
                                print(name, value, f'{time:.1f}', query)
                print(dataset_files)


    #for key in method_data:
    #    pass
def main(argv):
    globals().update(plot_config('config/plot.yaml'))
    set_pandas_display_options()
    output = get_data()
    plot_prob(output)
    plot_machine(output)
    plot_object(output)
    plot_instance(output)
    #test()
    #plot('split_threshold')
    #plot('bin_size')
    #plot('dominating_instance')
    #plot('instance')
    #plot('object')
    #plot('machine')

if __name__ == "__main__":
    main(sys.argv[1:])
