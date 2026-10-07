---
description: APR分配の月次まとめを日次記録から作る
argument-hint: <YYYY-MM(空なら先月)>
---
対象月: $ARGUMENTS(空なら先月)

`30_Areas/Investing/APR/` のその月の記録を全て読み、`Templates/AprMonthly.md` に沿って `30_Areas/Investing/APR/YYYY-MM 月次まとめ.md` を作成してください。

- 数字は日次記録にあるものだけを使い、集計の計算式を添える
- 記録がない日、`draft` のまま残っている日、通知が「未」の投資家を必ず列挙する
- OCR値と手入力値の差異メモがあれば転記する
