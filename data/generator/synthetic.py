import os
import pandas as pd
import numpy as np
from pathlib import Path
data_dir = Path(__file__).resolve().parent.parent / 'dataset'
data_dir.mkdir(parents=True, exist_ok=True)

def randomPeak(min, max, dim):
    average = np.average(np.random.uniform(0, 1, dim))
    return average * (max - min) + min

def randomNormal(med, var, dim):
    return randomPeak(med - var, med + var, dim)

def generate(type, num_objects, num_instances, dimension, precision):
    total_instances = num_objects * num_instances
    prob_list = np.zeros((num_objects, num_instances))
    inst_idx_list = np.arange(total_instances, dtype = np.object)
    obj_idx_list = np.arange(num_objects, dtype = np.object).repeat(num_instances, axis = 0)
    if num_instances != 1:
        for obj_id in range(num_objects):
            prob_list[obj_id] = np.random.dirichlet(np.ones(num_instances), size = 1)
    else:
        for obj_id in range(num_objects):
            prob_list[obj_id] = np.random.uniform(0, 1, size = 1)
    if type == 'ind':
        features = ind(total_instances, num_objects, num_instances, dimension, precision)
    elif type == 'cor':
        features = cor(total_instances, num_objects, num_instances, dimension, precision)
    elif type == 'anticor':
        features = anticor(total_instances, num_objects, num_instances, dimension, precision)
    else:
        raise
    prob_list = prob_list.reshape((total_instances, 1))
    save(type, num_objects, num_instances, obj_idx_list, inst_idx_list, prob_list, features, dimension)

def ind(total_instances, num_objects, num_instances, dimension, precision):
    features = np.round(np.random.uniform(0, 1, (num_objects, dimension)), precision).repeat(num_instances, axis = 0)
    mu = 0.1
    sigma = 0.025
    differences = np.random.normal(mu, sigma, (total_instances, dimension))
    features = abs(features + differences)
    return features

def cor(total_instances, num_objects, num_instances, dimension, precision):
    features = np.zeros((num_objects, dimension))
    counter = 0
    x = np.zeros(dimension)
    hyperplane_center = 0
    while counter < num_objects:
        hyperplane_center = randomPeak(0, 1, dimension)
        x[:] = hyperplane_center
        l = hyperplane_center if hyperplane_center <= 0.5 else 1.0 - hyperplane_center
        for d in range(dimension):
            h = randomNormal(0, l, dimension)
            x[d] += h
            x[(d + 1) % dimension] -= h

        flag = False
        for d in range(dimension):
            if x[d] < 0 or x[d] >= 1:
                flag = True
                break

        if flag:
            continue

        features[counter] = x
        counter += 1
        if counter % 10000 == 0:
            print(f'{counter} / {num_objects}')
    features = features.repeat(num_instances, axis = 0)
    mu = 0.1
    sigma = 0.025
    differences = np.random.normal(mu, sigma, (total_instances, dimension))
    features = abs(features + differences)
    return np.round(features, precision)

def anticor(total_instances, num_objects, num_instances, dimension, precision):
    features = np.zeros((num_objects, dimension))
    counter = 0
    x = np.zeros(dimension)
    while counter < num_objects:
        v = randomNormal(0.5, 0.25, dimension)
        x[:] = v
        l = v if v <= 0.5 else 1.0 - v
        for d in range(dimension):
            h = np.random.uniform(-l, l)
            x[d] += h
            x[(d + 1) % dimension] -= h

        flag = False
        for d in range(dimension):
            if x[d] < 0 or x[d] >= 1:
                flag = True
                break

        if flag:
            continue

        features[counter] = x
        counter += 1
        if counter % 10000 == 0:
            print(f'{counter} / {num_objects}')
    features = features.repeat(num_instances, axis = 0)
    mu = 0.1
    sigma = 0.025
    differences = np.random.normal(mu, sigma, (total_instances, dimension))
    features = abs(features + differences)
    return np.round(features, precision)

def save(type, num_objects, num_instances, obj_idx_list, inst_idx_list, prob_list, features, dimension):
    filename = '%s_%sd_%s_%s.txt' % (type, dimension, num_objects, num_instances)
    final_columns = ['obj#', 'inst#', 'prob'] + ['dim%d' % i for i in range(dimension)]
    df = pd.DataFrame(columns = final_columns)
    df['obj#'] = obj_idx_list
    df['inst#'] = inst_idx_list
    df['prob'] = prob_list
    for i in range(dimension):
        df['dim%d' % i] = features[:, i]
    file_path = data_dir / filename
    df.to_csv(file_path, sep = ' ', header = None, index = False)
    np.set_printoptions(suppress=True)
    print(filename, df)
    print('coef', np.corrcoef(features.T))


num_objects = 200000
num_instances = 10
precision = 6
for dimension in [7]:
    generate('ind', num_objects, num_instances, dimension, precision)
    generate('cor', num_objects, num_instances, dimension, precision)
    generate('anticor', num_objects, num_instances, dimension, precision)
