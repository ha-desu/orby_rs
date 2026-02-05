<p align="center">
  <img src="https://github.com/user-attachments/assets/c3eec46e-f583-4084-9186-56d29d60fde3" width="128" alt="Orby Logo">
</p>

# Orby (Orbital Observer) v0.2.0 (Alpha) 🌌

🚨 **開発途中のアルファ版です。本番環境での利用は推奨しません。**

Orby（オービィ）は、128bit 固定長データ（UUID v7/v8 等）の走査・検索に特化した、Parallel Arrays方式のインメモリ・インデックスエンジンです。


## 🌌 The Concept
Orbyのイメージは、マルチプラッタのHDDと物理的に一本のバーで繋がった磁気ヘッドです。
ある次元のデータを探し当てた瞬間、他の次元の磁気ヘッドも同じ位置を指し示します。同期されたインデックスから100次元のデータでも1次元と同じ速度で引き抜けるはずです。
それに準えて、Orbital Observer（オービタル・オブザーバー）略して、Orbyとして命名しました。

## ✨ Key Features

### 1. Parallel Arrays Architecture
各次元（レーン）を独立したメモリ領域/ファイルとして管理します。
特定の次元のスキャンが他次元のキャッシュ効率を妨げず、高いCPUキャッシュヒット率を実現するはずです

### 2. Vault System (High-Speed Persistence)
- **SoA (Structure of Arrays) Persistence**: 従来の行指向ではなく、列（レーン）ごとに独立したバイナリファイル (`lane_N.bin`) として保存します。
- **Parallel Writes**: `rayon` によるスレッドプールを活用し、数万レーンあっても全CPUコアを使って並列に書き込みます。
- **Durability**: 書き込み時は必ず `fsync` を発行し、OSのキャッシュからディスクへの物理書き込みを保証します。
- **Deep Sleep**: `sleep()` コマンド一つで、メモリ上の全データを並列かつ安全にディスクへ退避できます。

### 3. Safety & Performance
- **Zero-Latency Synchronization**: 全次元が共通のカーソル（リングバッファのヘッド）を共有し、どの次元からでもO(1)で関連データへアクセス可能。
- **Auto-Compaction**: 削除時にデータをスライドさせて隙間を詰めるコンパクション機能をサポート。
- **Thread Safety**: 内部は `RwLock` で保護されており、安全に並行アクセスが可能。

---

## 🚀 Quick Start

```rust
use orby::{Orby, SaveMode, LogicMode};

// 1. エンジンの初期化 (1万レコード / 2次元)
let orby = Orby::builder("my_pulse_orbit")
    .capacity(10_000)
    .dimension(2) // ring_buffer_lane_count
    // 永続化設定: Vaultモード (ディレクトリ指定)
    .with_storage(SaveMode::Vault(Some("./data".into())))
    .logic_mode(LogicMode::RingBuffer)
    .build()
    .await?;

// 2. レーザー貫通インサート (Vertical Insert)
// Store0 と Store1 の同じヘッド位置に同時にパルスを焼き付ける
let id = 12345u128;
let val = 99999u128;
orby.insert_batch(vec![vec![id, val]]).await?;

// 3. 高速検索 (Parallel Search)
// 次元0 (ID列) をスキャンして条件に合うものを探す
// Rayonによる並列スキャンが自動的に走る
let results = orby.query_raw(|row| row[0].as_u128() == id, 10);

// 4. 永続化 (Deep Sleep)
// 全メモリデータを並列でディスクに書き出し、fsync で保証する
orby.sleep().await?;
```

---

## 📖 Operational Recipes

Orbyは設定（LogicMode, SaveMode, Compaction）の組み合わせで多様なユースケースに対応します。

### 1. The Pulse Streamer (無限循環ログ)
用途: ログ集計、最新ステータスの追跡
- `compaction(true)`: 削除時は詰める
- 古いデータはリングバッファの回転により自動的に上書き消滅

```rust
let streamer = Orby::builder("streamer")
    .capacity(1_000)
    .compaction(true) // 削除時に詰める
    .build().await?;
```

### 2. The Static Slot (固定スロット)
用途: ゲームのインベントリ、固定枠の管理
- `compaction(false)`: 削除時はゼロ埋め（墓標）してインデックスを維持
- インデックスが不変のIDとして機能

```rust
let inventory = Orby::builder("inventory")
    .capacity(100) // 最大スロット数
    .compaction(false) // インデックスを固定
    .build().await?;
```

## ⚠️ Architectural Constraints

1. **次元ごとの独立カーソル不可**: 全次元は常に同期しています。「1次元目はN番目、2次元目はM番目」という状態は持ちません。
2. **固定長データのみ**: 扱うデータは `u128` (PulseCell) 固定です。可変長データは外部のKVS等に保存し、OrbyにはそのIDやハッシュのみを格納することを推奨します。

---

## 🛠 Major Changes in v0.2.0
- **Parallel Arrays**: 内部構造を Row-Oriented から Column-Oriented (SoA) に完全刷新。
- **Vault System**: 旧スナップショット機能を廃止し、より堅牢で高速な Vault 永続化を採用。
- **Rayon Integration**: クエリおよび永続化の並列処理化。
- **Error Handling**: `thiserror` ベースの堅牢なエラー型定義 (`OrbyError`)。