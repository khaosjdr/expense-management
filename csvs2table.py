import os
import re
import unicodedata

import pandas as pd
import pdfplumber

# 全角スペースを半角スペースに、全角英数字を半角英数字に変換する関数


def normalize_text(text):
    # 全角を半角に
    text = text.replace("　", " ")
    text = text.replace("−", "-")
    # カンマを除外
    text = text.replace(',', '').replace('、', '')
    # 2つ以上のスペースを1つに
    text = re.sub(r'\s+', ' ', text)
    # 全角英数字を半角英数字に
    text = unicodedata.normalize("NFKC", text)
    # テキストは30文字以内
    text = text[:30]
    return text


def money_str2int(s):
    return int(s.replace('￥', '').replace(',', ''))


def main(year):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')

    data_rows = []

    # read amazon
    amazon_dir = os.path.join(data_dir, 'amazon')
    for file_name in os.listdir(amazon_dir):
        if not file_name.endswith('.pdf'):
            continue
        data_path = os.path.join(amazon_dir, file_name)
        with pdfplumber.open(data_path) as pdf:
            page = pdf.pages[0]
            lines = page.extract_text().splitlines()

        # 行抽出
        date_line = lines[lines.index('注文情報')+1]
        purchase_lines = lines[lines.index('税抜 税込 税込')+1:lines.index('税率 小計 税額 小計')-1]

        # 日付抽出
        date = date_line.split(' ')[-1]

        # 内容と金額抽出
        results = []
        current_group = []
        for line in purchase_lines:
            splits = line.split(' ')
            if '￥' in line and not line.startswith('値引'):
                if current_group:
                    results.append(current_group)
                *words, _, _, _, _, money = splits
                text = ''.join(words)
                money = money_str2int(money)
                current_group = [text, money]
            elif '￥' in line:
                # 値引パターン
                current_group[1] += money_str2int(splits[-1])
            elif '￥' not in line:
                # 内容の改行パターン
                text = ''.join(splits)
                current_group[0] = ''.join([current_group[0], text])

        # 最後のグループを追加
        if current_group:
            results.append(current_group)

        data_rows.extend([{'日付': date, '内容': text, '金額': money, '参照元': 'Amazon'}
                          for text, money in results])

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
    data_df['日付'] = pd.to_datetime(data_df['日付'], format='mixed')
    data_df['金額'] = data_df['金額'].astype(int)
    data_df['内容'] = data_df['内容'].apply(normalize_text)
    data_df = data_df[['日付', '参照元', '金額', '内容']]
    data_df = data_df[data_df['金額'] != 0]
    data_df = data_df[data_df['日付'].dt.year == year]
    data_df = data_df.sort_values(by=['日付']).reset_index(drop=True)

    os.makedirs('results', exist_ok=True)
    data_df.to_csv(os.path.join('results', f'result{year}.csv'), index=False)


if __name__ == '__main__':
    main(2024)
