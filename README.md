# expense-management

## 明細ファイルのダウンロード方法

| 入力 | 明細確定日 | 明細ダウンロード方法 | 出力ファイル名 |
| --- | --- | --- | --- |
| Amazon | 翌月1日 | アマゾン注文履歴フィルタを有効化し、Amazonの注文履歴のフィルタの年と対象月を選択。デジタルとデジタル以外を選択し、予約分と0円は除外。領収書印刷用画面を開きcsvダウンロード | amazon-order_*_yyyy-m.csv |
| JRE Card | 翌月25日※ | Viewカードサイトからログインしてご利用明細照会からcsvダウンロード | ご利用明細_yyyymm.csv |
| SMBC | 翌月1日 | 三井住友銀行にログイン後、対象の口座を選択し、期間を１か月間に設定して明細ダウンロード | meisai.csv |
| SMBC Card | 翌月10日 | ログイン後、ご利用明細からcsvダウンロード | yyyymm.csv |

* ※ 翌月1日にはリストアップ済みのため、1日に確認可能。

## 明細ファイルの配置方法

`data` 以下の各フォルダに配置する。

## 全体の購買ログ作成方法

以下のコマンドを実行

```text
python csv2table.py
```

結果が `results` 以下に生成される。
