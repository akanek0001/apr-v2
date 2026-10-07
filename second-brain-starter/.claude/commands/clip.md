---
description: Web Clipper で Inbox に溜まったクリップを分類・要約・リンクする
argument-hint: <空 = 全て / ノート名やキーワード>
---
対象: $ARGUMENTS(空なら全て)

`00_Inbox/` のうち frontmatter が `type: clip` かつ `status: unprocessed` のノートを対象にしてください。

1. 各クリップを `source` の URL で種別判定する
   - 動画サイト → `40_Resources/Videos/` / SNS → `40_Resources/Social/` / 記事・PDF・その他 → `40_Resources/Articles/`
   - 投資・APR関連の内容なら、`30_Areas/Investing/` への移動を提案する
2. クリップ本文**にある範囲だけ**で要約・要点を作る。本文が空・断片的なら「本文が取得できていない」と書き、推測で補わない
3. vault を検索し、関連ノートを2件以上 `[[ ]]` で提案する。同じ `source` の既存ノートがあれば重複として指摘する
4. **一覧表(クリップ名 / 移動先 / 要約案 / リンク案 / 重複)を先に見せ、承認を得てから**移動・編集する
5. 承認後: `Templates/Resource.md` の構成(要約 / 要点 / 自分の考え / 関連ノート / 出典)に整え、「自分の考え」欄は空欄のまま残す。frontmatter は `type`(article/video/social)に直し、`status: unread` にする。元の本文は消さずに末尾の「原文クリップ」に残す
6. 処理後、件数と移動先を報告する

既存ノートの削除・上書きはしない。
