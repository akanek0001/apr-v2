---
description: Gmail から重要メールの要点とTODOを抽出して Inbox に保存する
argument-hint: <検索条件 例: newer_than:2d is:important>
---
検索条件: $ARGUMENTS(空なら `newer_than:2d is:important`)

Gmail を検索し、該当メールごとに次をまとめて `00_Inbox/` に1ノートずつ作成してください。

- 件名・送信者・日付・要点(数行)・自分のTODO(期限つき)
- **全文コピーはしない**。認証コード、口座番号、パスワード等の機微情報は書かない
- 返信・送信・ラベル変更・削除は行わない(読み取り専用)
