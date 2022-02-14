# Using readlines()
import json
import csv
import pandas as pd
import numpy as np
import random
def transform_data():
    inst_cnt = -1
    last_id = ''
    obj_cnt = -1
    dict = {}
    dims = [
    ['rooms', '-rm'],
    ['sleep quality', '-slp'],
    ['value', '-vu'],
    ['service','-sv'],
    ['cleanliness','-cn'],
    ['location','-lc'],
    ['business service (e.g., internet access)','-bs'],
    ['check in / front desk','-cki'],
    ]
    writer_dict = {}
    with open("HotelRec.txt", "r") as f:
        line = f.readline()
        while line:
            data = json.loads(line)
            psize = len(data['property_dict'])
            if psize not in dict:
                dict[psize] = 0
            dict[psize] += 1
            if psize > 0:
                features = data['property_dict']
                key = ''
                inst_features = []
                for dim in dims:
                    if dim[0] in features:
                        key += dim[1]
                        inst_features.append(features[dim[0]])
                info = None
                if key not in writer_dict:
                    file_name = f'tripadvisor_{key}.txt'
                    fp = open(file_name, 'w')
                    writer = csv.writer(fp, delimiter =' ')
                    info = {'fp': fp, 'writer': writer, 'obj_cnt':-1, 'last_obj_id': -1, 'inst_cnt': -1}
                    writer_dict[key] = info
                else:
                    info = writer_dict[key]

                if info['last_obj_id'] != data['hotel_url']:
                    info['last_obj_id'] = data['hotel_url']
                    info['obj_cnt'] += 1
                info['inst_cnt'] += 1
                inst_features = [info['obj_cnt'], info['inst_cnt'], data['rating']] + inst_features
                info['writer'].writerow(inst_features)
            inst_cnt += 1
            if inst_cnt % 100000 == 0:
                print(inst_cnt)

            line = f.readline()
    for key in writer_dict:
        info = writer_dict[key]
        print(f"file: {key}| obj#: {info['obj_cnt']} | inst#: {info['inst_cnt']}")
        info['fp'].close()
    print(dict)

def d(minOccurence = 5, instancesPerObj = 5, noise = False):
    dims = [
    ['rooms', '-rm', 'rooms'],
    ['sleep quality', '-slp', 'sleep'],
    ['value', '-vu', 'value'],
    ['service','-sv', 'service'],
    ['cleanliness','-cn', 'clean'],
    ['location','-lc', 'loc'],
    ['business service (e.g., internet access)','-bs', 'bservice'],
    ['check in / front desk','-cki', 'ckin'],
    ]
    #file_name = 'tripadvisor_-vu-sv-lc.txt'
    file_name = 'tripadvisor_-rm-slp-vu-sv-cn-lc.txt'
    #file_name = 'tripadvisor_-rm-vu-sv-cn-lc.txt'
    #file_name = 'tripadvisor_-vu-sv.txt'
    temp_columns = ['rating']
    temp_new_columns = ['rating']
    new_mapping = {'rating':'rating'}
    for dim in dims:
        if file_name.find(dim[1]) != -1:
            temp_columns.append(dim[0])
            temp_new_columns.append(dim[2])
            new_mapping[dim[0]] = dim[2]
    new_columns = ['obj#', 'inst#'] + temp_new_columns
    final_columns = ['obj#', 'inst#', 'prob'] + temp_new_columns
    #print(columns)
    df = pd.read_csv(file_name, header = None, names= new_columns, sep = ' ')
    df = df.rename(columns = new_mapping)
    """
    i = 5.0
    while i >= 0.0:
        query = []
        for column in temp_new_columns:
            query.append('%s == %.2f' % (column, i))
        query = ' & '.join(query)
        query_df = df.query(query)
        df = df[~df.index.isin(query_df.index)]
        #print(df)
        i -= 1.0
        break
    """


    obj_inst_count = df['obj#'].value_counts()
    obj_set = obj_inst_count[obj_inst_count >= minOccurence].index
    df = df[df['obj#'].isin(obj_set)]
    #print(df)
    obj_inst_count = df['obj#'].value_counts().sort_index()
    old_total_instance = obj_inst_count.sum()
    print("total obj#:", len(obj_inst_count.index))
    print("old total instance#:", old_total_instance)
    old_feature_list = np.empty([len(temp_new_columns), old_total_instance], dtype=np.float64)
    i = 0
    for column in temp_new_columns:
        old_feature_list[i] = df[column]
        i += 1
    total_instance = len(obj_inst_count.index) * instancesPerObj
    print("new total instance#:", total_instance)
    obj_idx_list = np.empty([total_instance], dtype=np.int64)
    inst_idx_list = np.empty([total_instance], dtype=np.int64)
    prob_list = np.empty([total_instance], dtype=np.float64)
    new_feature_list = np.empty([len(temp_new_columns), total_instance], dtype=np.float64)

    old_inst_idx_counter = 0
    new_inst_idx_counter = 0
    obj_idx_counter = 0
    for inst_count in obj_inst_count:
        startingIndex = old_inst_idx_counter
        endingIndex = old_inst_idx_counter + inst_count
        sampled_list = random.sample(range(startingIndex, endingIndex), instancesPerObj)
        #print(startingIndex, endingIndex)
        inst_probs = np.random.dirichlet(np.ones(instancesPerObj))
        for prob in inst_probs:
            prob_list[new_inst_idx_counter] = prob
            obj_idx_list[new_inst_idx_counter] = obj_idx_counter
            inst_idx_list[new_inst_idx_counter] = new_inst_idx_counter
            j = 0
            for column in temp_new_columns:
                for sample in sampled_list:
                    new_feature_list[j, new_inst_idx_counter] = old_feature_list[j, sample]
                j += 1
            new_inst_idx_counter += 1
            if new_inst_idx_counter % 100000 == 0:
                print(new_inst_idx_counter)

        old_inst_idx_counter += inst_count
        obj_idx_counter += 1

    new_df = pd.DataFrame(columns = final_columns)
    new_df['obj#'] = obj_idx_list
    new_df['inst#'] = inst_idx_list
    new_df['prob'] = prob_list
    i = 0
    for column in temp_new_columns:
        new_df[column] = new_feature_list[i]
        i += 1
    mu, sigma = (0, 0.2)
    rd = np.zeros([total_instance], dtype=np.float64)
    if noise:
        rd = np.round(np.random.normal(mu, sigma, total_instance), 2)
    else:
        sigma = 0.00
    new_df['rating'] = abs(6 - new_df['rating'] + rd)
    for dim in dims:
        if dim[2] in df:
            print(dim[2])
            rd = np.zeros([total_instance], dtype=np.float64)
            if noise:
                rd = np.round(np.random.normal(mu, sigma, total_instance), 2)
            new_df[dim[2]] = abs(6 - new_df[dim[2]] + rd)
    numNonFeatureColumns = 3
    dimension = len(temp_new_columns)
    for tempDimension in range(2, dimension + 1):
        endingIndex = numNonFeatureColumns + tempDimension
        temp_df = new_df.iloc[:, : endingIndex]
        csv_file = f'TA_{tempDimension}d_{minOccurence}_{instancesPerObj}_std_{sigma}.txt'
        print(csv_file)
        print(temp_df)
        temp_df.to_csv(csv_file, sep = ' ', header = None, index = False, columns = final_columns[:endingIndex])

def truncate_dataset(num_object_list = [100000]):
    import glob
    import os
    #dims = ['obj#', 'inst#', 'prob', 'rooms', 'sleep', 'value', 'service', 'clean', 'loc', 'bservice', 'ckin'],
    #]
    files = glob.glob("./data/*.txt")
    for file in files:
        df = pd.read_csv(file, header = None, sep = ' ')
        for num_object in num_object_list:
            new_df = df[df.iloc[:, 0] < num_object]
            new_file = '%s_%d.txt' % (file[:-4], num_object)
            new_df.to_csv(new_file, sep = ' ', header = None, index = False)
            print(new_df)
            print(new_file)
#transform_data()
d(12, 3, True)
d(12, 5, True)
d(12, 7, True)
d(12, 9, True)
#truncate_dataset([75000, 100000, 125000, 150000])
"""
d(10, False)
d(15, False)
d(20, False)
d(25, True)
d(25, False)
"""
