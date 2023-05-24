import os

import bs4
import pandas as pd
import requests
import wget
from sqlalchemy import create_engine

site_url = r'http://www.eurasiancommission.org/'

page_ref_list = ['2015/12_180_1', '2016/12_180', '2017/12-',
                 '2018/12-180', '2019/12_180', '2020/12_180', '2021/12_180']


def load_data(file_suffix_list, url_page):
    for page_ref in page_ref_list:
        url = url_page + page_ref + '.aspx'
        folder_path = os.path.join(os.getcwd(), 'downloaded_data', page_ref[:4:])
        os.makedirs(folder_path, exist_ok=True)
        page = requests.get(url).text
        soup = bs4.BeautifulSoup(page, "html.parser")
        data = soup.findAll('a', href=True)
        for a in data:
            href = a['href']
            for suffix in file_suffix_list:
                if href.endswith(suffix):
                    file_name = href[href.rfind('/')+1::]
                    print(file_name)
                    if not os.path.exists(os.path.join(folder_path, file_name)):
                        wget.download(site_url + href, folder_path)


def join_data_intra(resulting_file_name, file_suffix_list, columns, usecols, skiprows, skipfooter):
    appended_data = []
    for address, dirs, files in os.walk(os.path.join(os.getcwd(), 'downloaded_data')):
        for name in files:
            for suffix in file_suffix_list:
                if name.endswith(suffix):
                    file_path = os.path.join(os.getcwd(), address, name)
                    df = pd.read_excel(file_path, names=columns,
                                       usecols=usecols, skiprows=skiprows, skipfooter=skipfooter)
                    df = df[~df['month'].isin(['I квартал', 'II квартал', 'I полугодие', 'III квартал', 'Январь – сентябрь'])]
                    df['year'] = address[-4::]
                    df = df.melt(id_vars=['month', 'year'], var_name='country_code', value_name='turnover' )

                    appended_data.append(df)

    appended_data = pd.concat(appended_data)

    folder_path = os.path.join(os.getcwd(), 'combined_data')
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, resulting_file_name)
    appended_data.to_excel(file_path, index=False)  # header=False
    return file_path


def join_data(resulting_file_name, file_suffix_list, columns, usecols, skiprows, skipfooter):
    appended_data = []
    for address, dirs, files in os.walk(os.path.join(os.getcwd(), 'downloaded_data')):
        for name in files:
            for suffix in file_suffix_list:
                if name.endswith(suffix):
                    file_path = os.path.join(os.getcwd(), address, name)
                    df = pd.read_excel(file_path, names=columns,
                                       usecols=usecols, skiprows=skiprows, skipfooter=skipfooter)
                    df['year'] = address[-4::]
                    df['country_code'] = name[-5]

                    appended_data.append(df)

    appended_data = pd.concat(appended_data)

    folder_path = os.path.join(os.getcwd(), 'combined_data')
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, resulting_file_name)
    appended_data.to_excel(file_path, index=False)  # header=False
    return file_path


def join_data_goods_groups(resulting_file_name, file_suffix_list, columns, usecols, skiprows, skipfooter):
    appended_data = []
    for address, dirs, files in os.walk(os.path.join(os.getcwd(), 'downloaded_data')):
        for name in files:
            for suffix in file_suffix_list:
                if name.endswith(suffix):
                    file_path = os.path.join(os.getcwd(), address, name)
                    df = pd.read_excel(file_path, names=columns,
                                       usecols=usecols, skiprows=skiprows, skipfooter=skipfooter)
                    df['year'] = address[-4::]
                    df['country_code'] = name[-5]
                    df['group'] = df['group'].str.strip().str.capitalize()
                    df.loc[df['group'] == 'Кожевенное сырье, пушнина и изделия их них', 'group'] = 'Кожевенное сырье, пушнина и изделия из них'

                    appended_data.append(df)

    appended_data = pd.concat(appended_data)

    folder_path = os.path.join(os.getcwd(), 'combined_data')
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, resulting_file_name)
    appended_data.to_excel(file_path, index=False)  # header=False
    return file_path


def data_to_database(file_path, table_name):
    engine = create_engine("postgresql+psycopg2://postgres:anna@localhost:6543/dwh", echo=True)
    df = pd.read_excel(file_path)
    df.to_sql(table_name, engine, schema='stg', index=False, if_exists='append')

intr = {
        'name': 'mutual_trade',
        'suffix_list': ['12_1.xls'],
        'columns': ['month', '1', '2', '3', '4', '5', '6'],
        'usecols': 'A, B, D, G, J, M, P',
        'skiprows': 26,
        'skipfooter': 3,
        'url':site_url + r'ru/act/integr_i_makroec/dep_stat/tradestat/tables/intra/Pages/',
    }
extr = {
    'name': 'overral_results',
    'suffix_list': [f'_1_{i}.xls' for i in range(10)],
    'columns': ['month', 'turnover', 'export', 'import'],
    'usecols': 'A,E:G',
    'skiprows': 19,
    'skipfooter': 8,
    'url': site_url + r'ru/act/integr_i_makroec/dep_stat/tradestat/tables/extra/Pages/',
}

extr_goods = {
    'name': 'goods_groups',
    'suffix_list': [f'_3_{i}.xls' for i in range(10)],
    'url': site_url + r'ru/act/integr_i_makroec/dep_stat/tradestat/tables/extra/Pages/',
    'usecols': 'B,E,J',
    'skiprows': 5,
    'skipfooter': 0,
    'columns': ['group', 'export', 'import']
}

load_data(file_suffix_list=extr_goods['suffix_list'], url_page=extr_goods['url'])
file_path = join_data_goods_groups(resulting_file_name=extr_goods['name'] + '.xls',
                      file_suffix_list=extr_goods['suffix_list'],
                      columns=extr_goods['columns'],
                      usecols=extr_goods['usecols'],
                      skiprows=extr_goods['skiprows'],
                      skipfooter=extr_goods['skipfooter'])
data_to_database(file_path, table_name=extr_goods['name'])

load_data(file_suffix_list=intr['suffix_list'], url_page=intr['url'])
file_path = join_data_intra(resulting_file_name=intr['name'] + '.xls',
                            file_suffix_list=intr['suffix_list'],
                            columns=intr['columns'],
                            usecols=intr['usecols'],
                            skiprows=intr['skiprows'],
                            skipfooter=intr['skipfooter'])
data_to_database(file_path, table_name=intr['name'])

load_data(file_suffix_list=extr['suffix_list'], url_page=extr['url'])
file_path = join_data(resulting_file_name=extr['name'] + '.xls',
                      file_suffix_list=extr['suffix_list'],
                      columns=extr['columns'],
                      usecols=extr['usecols'],
                      skiprows=extr['skiprows'],
                      skipfooter=extr['skipfooter'])
data_to_database(file_path, table_name=extr['name'])














#
# import os
#
# import bs4
# import pandas as pd
# import requests
# import wget
# from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey, BigInteger
# from sqlalchemy import create_engine
#
# site_url = r'http://www.eurasiancommission.org/'
# url_extra = site_url + r'ru/act/integr_i_makroec/dep_stat/tradestat/tables/extra/Pages/'
# url_intra = site_url + r'ru/act/integr_i_makroec/dep_stat/tradestat/tables/intra/Pages/'
#
#
# page_ref_list = ['2015/12_180_1', '2016/12_180', '2017/12-',
#                  '2018/12-180', '2019/12_180', '2020/12_180', '2021/12_180']
#
# # def load_data_all(url_base, file_suffix_list):
# #     for page_ref in page_ref_list:
# #         url = url_base + page_ref + '.aspx'
# #         folder_path = os.path.join(os.getcwd(), 'downloaded_data', page_ref[:4:])
# #         os.makedirs(folder_path, exist_ok=True)
# #         page = requests.get(url).text
# #         soup = bs4.BeautifulSoup(page, "html.parser")
# #         data = soup.findAll('a', href=True)
# #         for a in data:
# #             href = a['href']
# #             for suffix in file_suffix_list:
# #                 if href.endswith(suffix):
# #                     # file_name = href[-15::] !!!!!!!!!!!!??? start_of_name = 15 or 13
# #                     file_name = href[-13::]
# #                     print(file_name)
# #                     if not os.path.exists(os.path.join(folder_path, file_name)):
# #                         wget.download(site_url + href, folder_path)
#
#
# def load_data(file_suffix_list):
#     for page_ref in page_ref_list:
#         url = url_extra + page_ref + '.aspx'
#         folder_path = os.path.join(os.getcwd(), 'downloaded_data', page_ref[:4:])
#         os.makedirs(folder_path, exist_ok=True)
#         page = requests.get(url).text
#         soup = bs4.BeautifulSoup(page, "html.parser")
#         data = soup.findAll('a', href=True)
#         for a in data:
#             href = a['href']
#             for suffix in file_suffix_list:
#                 if href.endswith(suffix):
#                     file_name = href[-13::]
#                     print(file_name)
#                     if not os.path.exists(os.path.join(folder_path, file_name)):
#                         wget.download(site_url + href, folder_path)
#
#
# def join_data_intra(resulting_file_name, file_suffix_list, columns, usecols, skiprows, skipfooter):
#     appended_data = []
#     for address, dirs, files in os.walk(os.path.join(os.getcwd(), 'downloaded_data')):
#         for name in files:
#             for suffix in file_suffix_list:
#                 if name.endswith(suffix):
#                     file_path = os.path.join(os.getcwd(), address, name)
#                     df = pd.read_excel(file_path, names=columns,
#                                        usecols=usecols, skiprows=skiprows, skipfooter=skipfooter)
#                     df = df[~df['month'].isin(['I квартал', 'II квартал', 'I полугодие', 'III квартал', 'Январь – сентябрь'])]
#                     df['year'] = address[-4::]
#                     df = df.melt(id_vars=['month', 'year'], var_name='country_code', value_name='turnover' )
#
#                     appended_data.append(df)
#
#     appended_data = pd.concat(appended_data)
#
#     folder_path = os.path.join(os.getcwd(), 'combined_data')
#     os.makedirs(folder_path, exist_ok=True)
#     file_path = os.path.join(folder_path, resulting_file_name)
#     appended_data.to_excel(file_path, index=False)  # header=False
#     return file_path
#
#
# def join_data(resulting_file_name, file_suffix_list, columns, usecols, skiprows, skipfooter):
#     appended_data = []
#     for address, dirs, files in os.walk(os.path.join(os.getcwd(), 'downloaded_data')):
#         for name in files:
#             for suffix in file_suffix_list:
#                 if name.endswith(suffix):
#                     file_path = os.path.join(os.getcwd(), address, name)
#                     df = pd.read_excel(file_path, names=columns,
#                                        usecols=usecols, skiprows=skiprows, skipfooter=skipfooter)
#                     df['year'] = address[-4::]
#                     df['country_code'] = name[-5]
#
#                     appended_data.append(df)
#
#     appended_data = pd.concat(appended_data)
#
#     folder_path = os.path.join(os.getcwd(), 'combined_data')
#     os.makedirs(folder_path, exist_ok=True)
#     file_path = os.path.join(folder_path, resulting_file_name)
#     appended_data.to_excel(file_path, index=False)  # header=False
#     return file_path
#
#
# def data_to_database(file_path, table_name):
#     engine = create_engine("postgresql+psycopg2://postgres:anna@localhost:6543/dwh", echo=True)
#     df = pd.read_excel(file_path)
#     df.to_sql(table_name, engine, schema='stg', index=False, if_exists='append')
#
# intr = {
#         'name': 'mutual_trade',
#         'suffix_list': ['12_1.xls'],
#         'columns': ['month', '1', '2', '3', '4', '5', '6'],
#         'usecols': 'A, B, D, G, J, M, P',
#         'skiprows': 26,
#         'skipfooter': 3,
#     }
# extr = {
#     'name': 'overral_results',
#     'suffix_list': [f'_1_{i}.xls' for i in range(10)],
#     'columns': ['month', 'turnover', 'export', 'import'],
#     'usecols': 'A,E:G',
#     'skiprows': 19,
#     'skipfooter': 8,
# }
#
# data_dicts_list = [
# #     {
# #     'name': 'overral_results',
# #     'suffix_list': [f'_1_{i}.xls' for i in range(10)],
# #     'columns': ['month', 'turnover', 'export', 'import'],
# #     'usecols': 'A,E:G',
# #     'skiprows': 19,
# #     'skipfooter': 8,
# # },
# #     {
# #         'name': 'geo_distribution',
# #         'suffix_list': [f'_2_{i}.xls' for i in range(10)],
# #         'columns': ['country', 'turnover', 'export', 'import'],
# #         'usecols': 'A,F:H',
# #         'skiprows': 4,
# #         'skipfooter': 0,
# #     },
#     {
#         'name': 'mutual_trade',
#         'suffix_list': ['12_1.xls'],
#         'columns': ['month', '1', '2', '3', '4', '5', '6'],
#         'usecols': 'A, B, D, G, J, M, P',
#         'skiprows': 26,
#         'skipfooter': 3,
#     }
# ]
#
# for metadata in data_dicts_list:
#     # load_data(file_suffix_list=metadata['suffix_list'])
#     # file_path = join_data(resulting_file_name=metadata['name'] + '.xls', file_suffix_list=metadata['suffix_list'],
#     #                             columns=metadata['columns'], usecols=metadata['usecols'], skiprows=metadata['skiprows'],
#     #                             skipfooter=metadata['skipfooter'])
#
#     load_data(file_suffix_list=metadata['suffix_list'])
#     file_path = join_data_intra(resulting_file_name=metadata['name'] + '.xls',
#                                 file_suffix_list=metadata['suffix_list'],
#                                 columns=metadata['columns'],
#                                 usecols=metadata['usecols'],
#                                 skiprows=metadata['skiprows'],
#                                 skipfooter=metadata['skipfooter'])
#     data_to_database(file_path, table_name=metadata['name'])
#
#



