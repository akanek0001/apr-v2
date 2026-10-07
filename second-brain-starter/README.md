# 第二の脳 スターター vault (Claude Code × Obsidian)

## セットアップ
1. このフォルダごと、手元の好きな場所へコピー(例: `~/MyBrain`)
2. Obsidian で「フォルダを vault として開く」
3. 設定 → コアプラグイン → テンプレートを有効化し、テンプレートフォルダを `Templates` に
4. ターミナルで `cd ~/MyBrain && git init && claude`
5. `CLAUDE.md` の「私について」を自分用に書き換える

## 使い方
| コマンド | 内容 |
|---|---|
| `/daily` | 今日のデイリーノートを作成(予定・未完了タスクを引き継ぎ) |
| `/digest <URL/PDF>` | 記事・動画・SNS・PDF を要約して保存 |
| `/meeting` | Fireflies の会議録をノート化 |
| `/mail` | Gmail の重要メールを要点だけ Inbox へ |
| `/invest <対象と内容>` | 投資の判断・情報を投資ノートに記録(売買推奨なし) |
| `/invest-review` | 投資の月次レビューを作成 |
| `/apr-log <日付/数値/画像>` | APR分配の日次記録を作成(通知は送らない) |
| `/apr-month` | APR分配の月次まとめを作成 |
| `/clip` | Web Clipper で溜めたクリップを分類・要約・リンク(`Clipper/README.md` 参照) |
| `/inbox` | Inbox の分類を提案(承認後に移動) |
| `/connect <ノート>` | 関連ノートへのリンクを提案 |
| `/ask <質問>` | vault のノートだけを根拠に回答 |
| `/weekly` | 週次レビューを作成 |

## 日々の流れ
- 朝: `/daily`
- 日中: 気になったURLは `/digest`、会議後は `/meeting`
- 夜〜週末: `/inbox` で整理、週1で `/weekly`

## 外部サービス連携
Gmail / Google Calendar / Fireflies は、Claude の「コネクタ」を有効にしたアカウントで Claude Code を使えば `/mail` `/daily` `/meeting` から呼べます。未接続なら該当コマンドが使えない旨を返すので、先に claude.ai の設定でコネクタを接続してください。

## おすすめプラグイン(任意)
Dataview(一覧表示)/ Templater(高度なテンプレート)/ Obsidian Git(自動バックアップ)/ Web Clipper(ブラウザからInboxへ保存)

## 安全運用
- vault は必ず git 管理(Claude の編集を差分で確認・巻き戻しできる)
- 機微情報(口座・パスワード等)は書かない。クラウド同期やコネクタ経由の情報の扱いに注意
