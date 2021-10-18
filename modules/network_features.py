import pandas as pd
from pandas import DataFrame
from py2neo import Graph
import configparser


def get_df(path: str, path_to_config: str) -> DataFrame:
    config = configparser.ConfigParser()
    config.read(path_to_config)

    graph = Graph(password=config["Graph"]["password"], bolt_port=config["Graph"]["bolt_port"], http_port=config["Graph"]["http_port"])
    # Use cypher query to get data from all the nodes
    query = config["Graph"]["query"]

    data = graph.run(query)
    valueDict = {}
    for d in data:
        valueDict[d['id']] = {'degree': d['degree'], 'pagerank': d['pagerank'], 'community': d['community']}
    # Helper functions to add network features to input dataframe

    def add_degree(x):
        return valueDict[x.split("'")[1]]['degree']

    def add_community(x):
        return str(valueDict[x.split("'")[1]]['community'])  # cast to string for one-hot encoding

    def add_pagerank(x):
        return valueDict[x.split("'")[1]]['pagerank']

    # Read in a new dataframe and add netork features
    df = pd.read_csv(path)

    df['merchDegree'] = df.merchant.apply(add_degree)
    df['custDegree'] = df.customer.apply(add_degree)
    df['custPageRank'] = df.customer.apply(add_pagerank)
    df['merchPageRank'] = df.merchant.apply(add_pagerank)
    df['merchCommunity'] = df.merchant.apply(add_community)
    df['custCommunity'] = df.customer.apply(add_community)
    return df
