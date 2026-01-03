---
name: codegen-feature
description: 機能追加を、README+Issue起点でTDD（codegen-test準拠）し、既存実装の流儀に合わせて、Serena MCPのセマンティック検索/編集を活用しつつ、高性能かつデータ競合に配慮して実装する。
---

# Codegen Feature Skill
## 目的
- README.md と Issue を根拠に、最小の変更で品質の高い Go 実装を作る。
- **テスト駆動**で進め、**既存コードの流儀**（命名・設計・構造・エラーハンドリング）に揃える。
- Serena MCP のツールで「読む量を減らし、正確に探して編集」する。

## いつ使うか
- Issue に基づく新規開発が必要なとき。
- パフォーマンスやデータ競合（race）に注意が必要な変更を含むとき。

## 成果物（期待するアウトプット）
- 失敗→成功へ遷移する **テスト**（generate-test skill 準拠）
- テストを通す **プロダクションコード**
  - テスト結果と静的解析が全て完了していること
- README/コメント/ドキュメント更新（ただし最小限）

---

## 実行手順（必ずこの順で）

### 0) 変更前の安全策
- 既存 API/挙動を壊さない
  - Issue/README に根拠がある場合のみ変更する。
  - 変更が必要な場合は、確認を求める。
- 変更範囲は最小。ついでのリファクタは絶対に行わない。
  - refactorが必要な場合は別Issueを作成する。
  - Issueの作成にはgithub mcpを利用する。

### 1) Issue を特定して要求を確定する（README + Issue を読む）
1. **現在のブランチ名**を取得する:
  - `git rev-parse --abbrev-ref HEAD`
2. ブランチ名から **Issue番号を抽出**する（例）:
  - `feature/issue-<issue_number>-`
3. Issue を読む
  - github mcpを利用する。
4. README.md / CONTRIBUTING / docs から **期待する使い方・制約・互換性**を確認する。
5. 受け入れ条件を「箇条書き」で確定し、**テスト観点**に変換する。
  - テスト観点は`codegen-test`skillに準拠する。

> Issue番号が抽出できない場合は、README/Issue一覧/PR/コミットメッセージから手がかりを探し、それでも不明なら「どのIssueを対象にするか」をユーザーに確認する。

### 2) 既存コードの流儀を探す（grep + Serena）
**目的:** 既存パターン（構造体、エラー、戻り値、命名、テストスタイル）に合わせる。

- まず grep / git grep で “入口” を作る:
  - `grep -En "keyword|TypeName|funcName" -r .`
  - `grep -En --include='*.go' "keyword|TypeName|funcName" -r .`
  - `git grep -nE "keyword|TypeName|funcName" -- '*.go'`
- 次に Serena MCP を使い、読み過ぎずに “正解の場所” を特定する:
  - `get_symbols_overview`（プロジェクトの主要シンボル俯瞰）
  - `find_symbol`（型/関数/メソッド定義へ）
  - `find_referencing_symbols`（呼び出し箇所・利用箇所へ）
  - `insert_after_symbol` / `replace_symbol_body` 等で **ピンポイント編集**
  - 大きいファイルの全読みは避け、必要箇所だけ取得する

### 3) テストを先に作る（generate-test skill に従う）
- **最初にテストを追加**し、失敗することを確認する（red）。
- テスト方針:
  - テーブル駆動（正常/異常/境界値）
  - 依存を注入できる設計（testable）
    - ただし、interfaceを多用しないこと。
    - シンプルな設計を心がけること。
  - 外部I/Oは interface 化・mock 化・in-memory 化する
    - 外部モジュールは利用しないこと。
- この工程は `codegen-test` skill の指示を **最優先**で適用する。

### 4) 実装する（最小・高性能・Goらしく）
**設計原則**
- 命名は **短く・一語で分かる**もの（曖昧語や長すぎる複合語を避ける）
- gopls/gofmt に従い、標準ライブラリ優先
- if/elseのネストを避け、early return/ハッピーパスを利用したコードにする。
- ロジックより **データ構造**を先に考える（性能・保守性が上がる）

**性能チェックリスト（必要なものを選んで適用）**
- 余計なアロケーションを避ける:
  - スライスは必要なら `make([]T, 0, n)` で capacity 予約
  - ループ内で `append` し続けるなら capacity を見積もる
- set には `map[string]struct{}` を使う（値を持たない）  
- “ゼロコピー” を意識:
  - `[]byte`↔`string` 変換をむやみに繰り返さない（境界で一回に寄せる）
  - 大きいデータは参照渡し/スライスで扱う
- ホットパスで `fmt.Sprintf` を多用しない。必要なら `strings.Builder` や `bytes.Buffer` を検討する。

### 5) テスト・静的チェック・レースチェック
- テスト(race込み)
  - `task go:test`
- 性能影響が大きい部分の修正時はテストを行う:
  - `task go:bench`

### 6) データ競合（race）を避けるルール
- 共有状態の読み書きを明示し、以下のいずれかで守る:
  - mutex / RWMutex
  - channel による所有権移譲
  - atomic（適用条件を満たす場合のみ）
- “見かけ上安全” な map/slice の共有をしない（読み取り専用でも構築タイミングに注意）。
- 競合しやすい箇所は **テストで並行実行**（`t.Parallel()` や goroutine）して再現性を上げる。

---

## 最後に出力するレポート（短く）
- レポートとしてマークダウンで出力する
  - ファイル名：`<Issue番号>_<日時>_feature_report.md`
- Issue要約（受け入れ条件）
- 変更点（ファイル単位）
- 追加したテストの観点
- 実行したコマンド
- パフォーマンス/競合面で気をつけた点（該当があれば）