# 引き継ぎメモ (クラウド → ローカル Claude Code)

この vault スターターはクラウド上の Claude Code で作成しました。以降はローカルの Claude Code で続けます。

## 現在の状態 (main にマージ済み)
- `CLAUDE.md`: 運用ルール (PARA、上書き禁止、出典必須、機微情報禁止、投資は売買推奨なし)
- `Templates/`: Daily / Resource / Meeting / Project / Investment / InvestMonthly / AprDistribution / AprMonthly
- `.claude/commands/`: `/daily` `/digest` `/meeting` `/mail` `/inbox` `/connect` `/ask` `/weekly` `/invest` `/invest-review` `/apr-log` `/apr-month` `/clip`
- `Clipper/`: Obsidian Web Clipper 用テンプレート (`Inbox-Clip.json`) と設定手順

## 未完了 (次にやること)
1. **動画クリップの扱いを「AI動画作成の手順書作成」に合わせる**
   - 元資料は作成者のPCのデスクトップにあり、クラウド側からは読めなかった
   - ローカルで元資料を読み、`.claude/commands/clip.md` と `digest.md` の動画の項目、`Templates/Resource.md` (`type: video`) に構成を反映する
   - 出力は「要約ノート」形式のまま、構成・書き方だけを手順書に寄せる
2. **動作確認 (全て未実施)**
   - Web Clipper に `Inbox-Clip.json` を取り込めるか (スキーマは更新で変わる可能性あり。ダメなら `Clipper/README.md` の手入力設定)
   - 各スラッシュコマンドの実機テスト
   - Gmail / Calendar / Fireflies のコネクタ接続 (claude.ai 側で接続済みのアカウントでログイン)
3. **APR分配の計算ルールの確定**
   - `/apr-log` は、Asset_Ratio をどう使って分配額を出すかのルールを渡されるまで分配額を空欄にして質問する設計
   - ルールが決まったら `CLAUDE.md` の「APR分配の記録」に書き込む

## 設計上の方針 (変えない前提)
- 既存ノートは削除・上書きしない。移動や一括編集は一覧を見せて承認を取ってから
- 読み取れない値・取得できない内容は推測せず「不明」とする
- 「自分の考え」欄・投資の撤退条件は本人が書く。Claude は空欄を指摘するだけ
- APR分配の記録の正本は Sheets 側。vault は控えと振り返り用
- 投資家名・金額は vault の外へ共有しない

## ローカルでの始め方
```bash
git clone https://github.com/akanek0001/apr-v2.git
cp -r apr-v2/second-brain-starter ~/MyBrain
cd ~/MyBrain && git init && git add -A && git commit -m "init vault"
claude
```
Obsidian で `~/MyBrain` を vault として開き、コアプラグイン「テンプレート」のフォルダを `Templates` に設定する。
`apr-v2` 本体 (`app.py` など) とは別物なので、vault は `apr-v2` の外に置く。

## 最初に Claude Code へ渡す依頼文 (そのまま使える)
```
この vault の CLAUDE.md と HANDOFF.md を読んで。
HANDOFF.md の「未完了 1」を進めたい。
デスクトップの「AI動画作成の手順書作成」(パス: ~/Desktop/〇〇)を読み、
構成と書き方の特徴を要約して見せて。
そのうえで clip.md / digest.md / Templates/Resource.md(video) への反映案を出し、
承認後に反映して。
```
