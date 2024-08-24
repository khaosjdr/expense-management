import os
import re
import csv
import pandas as pd

data_dir = os.path.join(os.path.dirname(__file__), 'data')

data_rows = []

# read amazon
amazon_dir = os.path.join(data_dir, 'amazon')
for file_name in os.listdir(amazon_dir):
    if not file_name.endswith('.csv'):
        continue
    data_path = os.path.join(amazon_dir, file_name)
    df = pd.read_csv(data_path)[['注文日', '商品名', '商品小計', '商品URL']]
    df = df[~df['商品小計'].isna()]
    df.rename(columns={
        '注文日': 'date',
        '商品名': 'desc',
        '商品小計': 'JPY',
        '商品URL': 'memo'
    }, inplace=True)
    df['source'] = 'Amazon'
    df = df[['date', 'source', 'desc', 'JPY', 'memo']]
    data_rows.extend(df.to_dict(orient='records'))

# read smbc card
smbc_dir = os.path.join(data_dir, 'smbc_card')
for file_name in os.listdir(smbc_dir):
    if not file_name.endswith('.csv'):
        continue
    data_path = os.path.join(smbc_dir, file_name)
    with open(data_path, 'r', encoding='shiftjis') as f:
        lines = f.read().splitlines()
    for line in lines:
        splits = line.split(',')
        if not re.match(r'\d{4}/\d{2}/\d{2}', splits[0]):
            continue
        date, desc, _, _, _, jpy, *_ = splits
        if 'ＡＭＡＺＯＮ．ＣＯ．ＪＰ' in desc:
            continue
        data_rows.append({
            'date': date,
            'source': 'smbc_card',
            'desc': desc,
            'JPY': float(jpy)
        })

# read jre card
jre_dir = os.path.join(data_dir, 'jre_card')
for file_name in os.listdir(jre_dir):
    if not file_name.endswith('.csv'):
        continue
    data_path = os.path.join(jre_dir, file_name)
    with open(data_path, 'r', encoding='shiftjis') as f:
        reader = csv.reader(f)
        for splits in reader:
            if not (len(splits) > 0 and re.match(r'\d{4}/\d{2}/\d{2}', splits[0]) and splits[4].replace(',', '').isdigit()):
                continue
            date, desc, _, _, jpy, *_ = splits
            jpy = jpy.replace(',', '')
            if float(jpy) <= 0:
                continue
            data_rows.append({
                'date': date,
                'source': 'jre_card',
                'desc': desc,
                'JPY': float(jpy)
            })

data_df = pd.DataFrame(data_rows)
data_df['date'] = pd.to_datetime(data_df['date'])
data_df['JPY'] = data_df['JPY'].astype(int)
data_df = data_df.sort_values(by='date').reset_index(drop=True)

os.makedirs('results', exist_ok=True)
data_df.to_csv(os.path.join('results', 'result.csv'), index=False)
