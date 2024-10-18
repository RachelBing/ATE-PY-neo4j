# -*- coding: utf-8 -*-
from DataToNeo4jClass.DataToNeo4jClass import DataToNeo4j
import os
import pandas as pd
import numpy as np
#pip install py2neo==5.0b1 注意版本，要不对应不了

Test_relation_data = pd.read_excel('./refdata/test.xls', header=0)


def data_extraction():
    """节点数据抽取"""

    # 取出名称到list
    node_Tester_Resource_key = []
    for i in range(0, len(Test_relation_data)):
        node_Tester_Resource_key.append(Test_relation_data['测试机资源'][i])
    
    node_Test_Items_key = []
    for i in range(0, len(Test_relation_data)):
        node_Test_Items_key.append(Test_relation_data['测试项'][i])
        
    # 去除重复的名称
    node_Test_Items_key = pd.Series(node_Test_Items_key).dropna().drop_duplicates().tolist()
    node_Tester_Resource_key = pd.Series(node_Tester_Resource_key).dropna().drop_duplicates().tolist()

    # value抽出作node
    node_list_value = []
    for i in range(0, len(Test_relation_data)):
        for n in range(1, len(Test_relation_data.columns)):
            # 取出表头名称invoice_data.columns[i]
            node_list_value.append(Test_relation_data[Test_relation_data.columns[n]][i])
    # 去重
    node_list_value = list(set(node_list_value))
    # 将list中浮点及整数类型全部转成string类型
    node_list_value = [str(i) for i in node_list_value]

    return node_Test_Items_key, node_Tester_Resource_key, node_list_value

def relation_extraction():
    """联系数据抽取"""

    links_dict = {}
    Test_Items_list = []
    method_list = []
    Tester_Resource_list = []

    for i in range(0, len(Test_relation_data)):
        Test_Items_list.append(Test_relation_data[Test_relation_data.columns[0]][i])#测试项
        method_list.append(Test_relation_data[Test_relation_data.columns[5]][i])#测试方法
        Tester_Resource_list.append(Test_relation_data[Test_relation_data.columns[3]][i])#测试机资源

    # 将数据中int类型全部转成string
    Test_Items_list = [str(i) for i in Test_Items_list]
    method_list = [str(i) for i in method_list]
    Tester_Resource_list = [str(i) for i in Tester_Resource_list]

    # 整合数据，将三个list整合成一个dict
    links_dict['Test_Items'] = Test_Items_list
    links_dict['method'] = method_list
    links_dict['Tester_Resource'] = Tester_Resource_list
    
    
    # 找到包含nan的索引
    nan_indices = set()

    for value in links_dict.values():
        for index, item in enumerate(value):
            if isinstance(item, str) and item == 'nan':
                nan_indices.add(index)

    # 删除所有列表在nan_indices对应位置的元素
    for key in links_dict.keys():
        for index in sorted(nan_indices, reverse=True):  # 反向排序以避免索引错误
            del links_dict[key][index]
            
    # 将数据转成DataFrame
    df_data = pd.DataFrame(links_dict)
    print(df_data)
    return df_data


# relation_extraction()
create_data = DataToNeo4j()
extract_data = data_extraction()
create_data.create_node(extract_data[0], extract_data[1])
create_data.create_relation(relation_extraction())
