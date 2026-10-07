---
description: APR分配の日次記録を作る(スクリーンショットや数値から)
argument-hint: <日付 / プロジェクト名 / 数値やスクリーンショットのパス>
---
入力: $ARGUMENTS

1. `30_Areas/Investing/APR/` を検索し、同日・同プロジェクトの記録があれば追記、なければ `Templates/AprDistribution.md` から `YYYY-MM-DD プロジェクト名.md` を作る
2. スクリーンショットが渡されたら、Liquidity / Yesterday Profit / APR / USDC取引履歴を読み取って表に入れる。**読み取れない値は推測せず「不明」**とし、画像のパスを出典に残す
3. 投資家別の分配額は、私が示した Asset_Ratio または計算ルールがある場合のみ計算し、**計算式を必ず併記**する。ルールが無ければ空欄にして質問する
4. status は `draft` で作る。`confirmed` / `notified` への更新は私が確認してから
5. LINE への通知は送らない(記録のみ)

数字の正誤判断や運用の助言はしない。記録と整理が目的。
