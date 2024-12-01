import os
import re
import unicodedata

import pandas as pd

# 全角スペースを半角スペースに、全角英数字を半角英数字に変換する関数


def normalize_text(text):
    # 全角を半角に
    text = text.replace("　", " ")
    text = text.replace("−", "-")
    # 2つ以上のスペースを1つに
    text = re.sub(r'\s+', ' ', text)
    # 全角英数字を半角英数字に
    text = unicodedata.normalize("NFKC", text)
    return text


def main(year):
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
        df['内容'] = df.apply(
            lambda row: f'{row["商品名"]}({row["商品URL"]})' if pd.notna(row["商品URL"]) else row["商品名"],
            axis=1
        )
        df = df[['注文日', '内容', '商品小計']]
        df['参照元'] = 'Amazon'
        df.rename(columns={
            '注文日': '日付',
            '商品小計': '金額'
        }, inplace=True)
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
            if len(memo) > 0:
                description = f'{description} [{memo}]'
            data_rows.append({
                '日付': date,
                '内容': description,
                '金額': float(money),
                '参照元': 'SMBCカード'
            })

    # read smbc
    smbc_path = os.path.join(data_dir, 'smbc', 'meisai.csv')
    df = pd.read_csv(smbc_path, encoding='shiftjis')
    df = df[~df['お取り扱い内容'].isin(['ﾐﾂｲｽﾐﾄﾓｶ-ﾄﾞ (ｶ'])]
    df['取引金額'] = df['お引出し'].fillna(0) - df['お預入れ'].fillna(0)
    df = df[['年月日', 'お取り扱い内容', '取引金額']]
    df['参照元'] = '三井住友銀行'
    df.rename(columns={
        '年月日': '日付',
        'お取り扱い内容': '内容',
        '取引金額': '金額'
    }, inplace=True)
    data_rows.extend(df.to_dict(orient='records'))

    # マージ
    data_df = pd.DataFrame(data_rows)
    data_df['日付'] = pd.to_datetime(data_df['日付'])
    data_df['金額'] = data_df['金額'].astype(int)
    data_df['内容'] = data_df['内容'].apply(normalize_text)
    data_df = data_df.sort_values(by=['日付']).reset_index(drop=True)
    data_df = data_df[data_df['日付'].dt.year == year]

    os.makedirs('results', exist_ok=True)
    data_df.to_csv(os.path.join('results', f'result{year}.csv'), index=False)


if __name__ == '__main__':
    main(2024)
