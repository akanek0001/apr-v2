# Obsidian Web Clipper 取り込みフロー

ブラウザで気になったページを **ワンクリックで `00_Inbox/` に保存**し、あとで Claude が `/clip` で分類・要約・リンクします。

## 流れ
```
ブラウザ → Web Clipper → 00_Inbox/(status: unprocessed)
        → /clip → 40_Resources/ 配下へ移動 + 要約 + 関連リンク(承認後)
```

## セットアップ
1. ブラウザに [Obsidian Web Clipper](https://obsidian.md/clipper)(公式拡張)をインストール
2. 拡張機能の設定 → テンプレート → **インポート** で `Inbox-Clip.json` を読み込む
   - 取り込めない場合は、新規テンプレートを作り、下記の値を手入力する
     - ノート名: `{{title}}` / 保存先フォルダ: `00_Inbox` / 動作: 新規ノート作成
     - プロパティ: `type=clip`, `status=unprocessed`, `source={{url}}`, `site={{site}}`, `author={{author}}`, `published={{published}}`, `created={{date|date:"YYYY-MM-DD"}}`, `tags=clip`
     - 本文: `# {{title}}` / `> 出典: {{url}}` / `{{selection|blockquote}}` / `{{content}}`
   - ※ Web Clipper の仕様は更新されるため、変数名・取り込み形式が変わっている場合は最新の公式ドキュメントに合わせる
3. Vault 名を拡張機能に設定する(保存先の vault を選ぶ)
4. 記事の一部だけ残したい時は、テキストを選択してからクリップする(`{{selection}}` に入る)

## 使い方
- 保存したら放置してよい。整理はあとでまとめて `/clip`
- `/clip` は `status: unprocessed` のクリップだけを対象にする
- 動画 (YouTube等) や SNS は、ページから取れた範囲(タイトル・説明・投稿本文)だけを使う。字幕や本文が無ければ要約を作らず、その旨を書く

## 注意
- ログイン必須のページや有料記事は、クリップした本文の範囲だけを扱う
- 機微な情報を含むページ(口座画面・メール本文など)はクリップしない
