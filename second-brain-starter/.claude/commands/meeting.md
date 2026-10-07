---
description: Fireflies の会議録をノート化する
argument-hint: <会議名 / 日付 / 空欄で直近の会議>
---
対象: $ARGUMENTS(空なら直近の会議)

Fireflies から該当会議の文字起こし・要約を取得し、`Templates/Meeting.md` に沿って `40_Resources/Meetings/YYYY-MM-DD 会議名.md` を作成してください。

- 決定事項 / TODO(担当・期限つき)/ 論点 を必ず抽出する。発言者が不明な項目は「不明」とする
- 関連する `20_Projects/` があればリンクし、TODO は当日のデイリーノートへの転記を提案する
