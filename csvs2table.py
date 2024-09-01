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
    df.loc[df.index[df['商品名'].str.contains('返金')], '商品小計'] *= -1
    df.rename(columns={
        '注文日': 'Date',
        '商品名': 'Description',
        '商品小計': 'Money',
        '商品URL': 'Memo'
    }, inplace=True)
    df['Source'] = 'amazon'
    df = df[['Date', 'Source', 'Description', 'Money', 'Memo']]
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
        date, description, _, _, _, money, memo = splits
        if 'ＡＭＡＺＯＮ．ＣＯ．ＪＰ' in description:
            continue
        data_rows.append({
            'Date': date,
            'Source': 'smbc_card',
            'Description': description,
            'Money': float(money),
            'Memo': memo
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
            date, description, _, _, money, *_ = splits
            money = money.replace(',', '')
            if float(money) <= 0:
                continue
            data_rows.append({
                'Date': date,
                'Source': 'jre_card',
                'Description': description,
                'Money': float(money)
            })

# read smbc
smbc_path = os.path.join(data_dir, 'smbc', 'meisai.csv')
df = pd.read_csv(smbc_path, encoding='shiftjis')
df = df[~df['お取り扱い内容'].isin(['ｶ)ﾋﾞﾕ-ｶ-ﾄﾞ', 'ﾐﾂｲｽﾐﾄﾓｶ-ﾄﾞ (ｶ'])]
df['取引金額'] = df['お引出し'].fillna(0) - df['お預入れ'].fillna(0)
df['Source'] = 'smbc'
df = df[['年月日', 'Source', 'お取り扱い内容', '取引金額']]
df.rename(columns={
    '年月日': 'Date',
    'お取り扱い内容': 'Description',
    '取引金額': 'Money'
}, inplace=True)
data_rows.extend(df.to_dict(orient='records'))

data_df = pd.DataFrame(data_rows)
data_df['Date'] = pd.to_datetime(data_df['Date'])
data_df['Year'] = data_df['Date'].dt.year
data_df['Month'] = data_df['Date'].dt.month
data_df['Day'] = data_df['Date'].dt.day
data_df['Money'] = data_df['Money'].astype(int)
data_df = data_df.sort_values(by=['Date', 'Source', 'Money', 'Description']).reset_index(drop=True)
data_df = data_df[['Year', 'Month', 'Day', 'Source', 'Description', 'Money', 'Memo']]

data_df = data_df[data_df['Year'] == 2024]
data_df.drop_duplicates(inplace=True)

os.makedirs('results', exist_ok=True)
data_df.to_csv(os.path.join('results', 'result2024.csv'), index=False)
