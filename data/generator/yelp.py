
import json
import csv
import pandas as pd
import numpy as np
import random
"""
with open('yelp_academic_dataset_review.json') as json_file:
    data = json.load(json_file)
    for p in data['people']:
        print('Name: ' + p['name'])
        print('Website: ' + p['website'])
        print('From: ' + p['from'])
        print('')
"""

def create_raw_data():
    max_stars, max_useful, max_funny, max_cool = -1, -1, -1, -1
    dict = {}
    with open("yelp_academic_dataset_review.json", "r") as f:
        line = f.readline()
        cnt = 0
        while line:
            data = json.loads(line)
            business_id = data['business_id']
            stars = float(data['stars'])
            useful = float(data['useful'])
            funny = float(data['funny'])
            cool = float(data['cool'])
            max_stars = max(max_stars, stars)
            max_useful = max(max_useful, useful)
            max_funny = max(max_funny, funny)
            max_cool = max(max_cool, cool)
            if business_id not in dict:
                dict[business_id] = []
            dict[business_id].append([stars, useful, funny, cool])
            line = f.readline()
            cnt = cnt + 1
            if cnt % 10000 == 0:
                print("read", cnt)
    csv_file = f'yelp.txt'
    csv_columns = ['objId', 'stars', 'useful', 'funny', 'cool']
    with open(csv_file, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter =' ')
        objNum = 0
        numInstance = 0
        for data in dict:
            #size = len(dict[data])
            #probList = np.random.dirichlet(np.ones(size), size = 1)
            #if size == 1:
            #    probList[0][0] = 1.0
            i = 0
            for row in dict[data]:
                #rd = np.random.ranf((1, 4))
                stars, useful, funny, cool = row[0], row[1], row[2], row[3]
                stars = max_stars - stars
                useful = max_useful - useful
                funny = max_funny - funny
                cool = max_cool - cool
                writer.writerow([objNum, numInstance, stars, useful,funny, cool])
                i += 1
                numInstance += 1
            objNum += 1
    print("Done", objNum)

def create_data(minOccurence = 10, instances_per_obj = 10):
    columns = ['obj#', 'inst#', 'stars', 'useful', 'funny', 'cool']
    temp_new_columns = ['stars', 'useful', 'funny', 'cool']
    final_columns = ['obj#', 'inst#', 'prob', 'stars', 'useful', 'funny', 'cool']
    df = pd.read_csv('yelp.txt', sep = ' ', header = None, names = columns)
    df = df.set_index('inst#')
    obj_inst_count = df['obj#'].value_counts()
    obj_set = obj_inst_count[obj_inst_count >= minOccurence].index
    df = df[df['obj#'].isin(obj_set)]

    obj_inst_count = df['obj#'].value_counts().sort_index()
    old_total_instance = obj_inst_count.sum()
    print("total obj#:", len(obj_inst_count.index))
    print("old total instance#:", old_total_instance)
    old_feature_list = np.empty([len(temp_new_columns), old_total_instance], dtype=np.float64)
    i = 0
    for column in temp_new_columns:
        old_feature_list[i] = df[column]
        i += 1
    total_instance = len(obj_inst_count.index) * instances_per_obj
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
        sampled_list = random.sample(range(startingIndex, endingIndex), instances_per_obj)
        #print(startingIndex, endingIndex)
        inst_probs = np.random.dirichlet(np.ones(instances_per_obj))
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

    dimension = len(temp_new_columns)
    new_df = pd.DataFrame(columns = final_columns)
    new_df['obj#'] = obj_idx_list
    new_df['inst#'] = inst_idx_list
    new_df['prob'] = prob_list
    i = 0

    mu, sigma = (0, 0.2)
    for column in temp_new_columns:
        max_value = np.max(new_feature_list[i])
        min_value = np.min(new_feature_list[i])
        print(column, min_value, max_value)
        noise = rd = np.round(np.random.normal(mu, sigma, total_instance), 2)
        new_df[column] = abs(np.round((new_feature_list[i] - min_value) / (max_value - min_value) + noise, 2))
        i += 1

    csv_file = f'Yelp_4d_{minOccurence}_{instances_per_obj}.txt'
    new_df.to_csv(csv_file, sep = ' ', header = None, index = False)


    print(new_df)

#create_raw_data()
create_data()
#create_data(50000)
#create_data(100000)
#create_data(150000)
