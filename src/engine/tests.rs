use super::*;
use crate::row::PulseCellPack;

#[tokio::test]
async fn test_insert() {
    let label = "test_insert";
    // ring_buffer_lane_item_count=5 => capacity (rows) = 5
    let engine = Orby::new(label, 5, 2, SaveMode::MemoryOnly, LogicMode::RingBuffer)
        .await
        .unwrap();

    let val1 = (7u128 << 76) | 12345;
    let val2 = (7u128 << 76) | 67890;
    let row = PulseCellPack::new([val1, val2]);

    engine.insert_fixed(vec![row]).await.unwrap();
    assert_eq!(engine.len(), 1);
}

#[tokio::test]
async fn test_query_injection() {
    let label = "test_query";
    let engine = Orby::new(label, 5, 2, SaveMode::MemoryOnly, LogicMode::RingBuffer)
        .await
        .unwrap();

    engine
        .insert_batch(vec![[100, 1], [200, 2], [300, 3]])
        .await
        .unwrap();

    let results = engine.query_raw(|row| row[1].as_u128() == 2, 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0][0], 200);

    let results = engine.find_custom(0, 150, 250, 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0][0], 200);
}

#[tokio::test]
async fn test_query_iter() {
    let label = "test_iter";
    let engine = Orby::new(label, 5, 2, SaveMode::MemoryOnly, LogicMode::RingBuffer)
        .await
        .unwrap();

    engine
        .insert_batch(vec![[100, 1], [200, 2], [300, 3]])
        .await
        .unwrap();

    let mut iter = engine.query_iter(|row| row[1].as_u128() >= 2);
    let res1 = iter.next().unwrap();
    assert_eq!(res1[0], 300);
    let res2 = iter.next().unwrap();
    assert_eq!(res2[0], 200);
    assert!(iter.next().is_none());
}

#[tokio::test]
async fn test_update_and_upsert() {
    let label = "test_update";
    let engine = Orby::builder(label)
        .ring_buffer_lane_item_count(5) // 5 * 2 = 10
        .ring_buffer_lane_count(2)
        .logic_mode(LogicMode::RingBuffer)
        .build()
        .await
        .unwrap();

    engine
        .insert_batch(&[[1u128, 10u128], [2u128, 20u128]])
        .await
        .unwrap();

    let found = engine.update_by_id(0, 1, &[1, 11]).await;
    assert!(found);

    let results = engine.query_raw(|row| row[0].as_u128() == 1, 1);
    assert_eq!(results[0][1], 11);

    engine.upsert(0, 2, &[2, 22]).await.unwrap();
    let results2 = engine.query_raw(|row| row[0].as_u128() == 2, 1);
    assert_eq!(results2[0][1], 22);

    engine.upsert(0, 3, &[3, 30]).await.unwrap();
    assert_eq!(engine.len(), 3);
    let results3 = engine.query_raw(|row| row[0].as_u128() == 3, 1);
    assert_eq!(results3[0][1], 30);
}

#[tokio::test]
async fn test_get_at_and_take() {
    let label = "test_get_at";
    let engine = Orby::builder(label)
        .ring_buffer_lane_item_count(5) // 5 rows
        .ring_buffer_lane_count(1)
        .logic_mode(LogicMode::RingBuffer)
        .build()
        .await
        .unwrap();

    engine
        .insert_batch(&[[10u128], [20u128], [30u128]])
        .await
        .unwrap();

    assert_eq!(engine.get_at(0).unwrap()[0], 30);
    assert_eq!(engine.get_at(1).unwrap()[0], 20);
    assert_eq!(engine.get_at(2).unwrap()[0], 10);
    assert!(engine.get_at(3).is_none());

    let list = engine.take(2);
    assert_eq!(list.len(), 2);
    assert_eq!(list[0][0], 30);
    assert_eq!(list[1][0], 20);
}

#[tokio::test]
async fn test_find_indices() {
    let label = "test_indices";
    let engine = Orby::builder(label)
        .ring_buffer_lane_item_count(5)
        .ring_buffer_lane_count(1)
        .logic_mode(LogicMode::RingBuffer)
        .build()
        .await
        .unwrap();

    engine
        .insert_batch(&[[10u128], [20u128], [30u128]])
        .await
        .unwrap();

    let indices = engine.find_indices(|row| row[0].as_u128() > 15, 10);
    assert_eq!(indices.len(), 2);
    assert_eq!(indices[0], 0);
    assert_eq!(indices[1], 1);

    assert_eq!(engine.get_at(indices[0]).unwrap()[0], 30);
    assert_eq!(engine.get_at(indices[1]).unwrap()[0], 20);
}

#[tokio::test]
async fn test_storage_only_persistence() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_dir = std::env::temp_dir().join(format!("orby_test_storage_only_{}", now));
    let _ = tokio::fs::create_dir_all(&temp_dir).await;
    let label = "test_storage_only";

    let engine = Orby::builder(label)
        .ring_buffer_lane_item_count(5)
        .ring_buffer_lane_count(2)
        .logic_mode(LogicMode::RingBuffer)
        .with_storage(SaveMode::Direct(Some(temp_dir.clone())))
        .build()
        .await
        .unwrap();

    assert!(engine.inner.read().lanes[0].buffer.is_empty());

    let val1 = 12345_u128;
    let val2 = 67890_u128;
    engine.insert_batch(&[[val1, val2]]).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let row_opt = engine.get_at(0);
    assert!(row_opt.is_some());
    let row = row_opt.unwrap();
    assert_eq!(row[0], val1);
    assert_eq!(row[1], val2);

    assert_eq!(engine.len(), 1);

    let mut iter = engine.query_iter(|_| true);
    let row = iter.next().expect("Should have one row");
    assert_eq!(row[0], val1);
    assert_eq!(row[1], val2);
    assert!(iter.next().is_none());

    let _ = tokio::fs::remove_dir_all(temp_dir).await;
}

#[tokio::test]
async fn test_snapshot_generation() {
    let label = "test_snapshot";
    let engine = Orby::builder(label)
        .ring_buffer_lane_item_count(5)
        .ring_buffer_lane_count(2)
        .logic_mode(LogicMode::RingBuffer)
        .build()
        .await
        .unwrap();

    engine.insert_batch(&[[1, 2], [3, 4]]).await.unwrap();

    let temp_dir = std::env::temp_dir();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let snapshot_path = temp_dir.join(format!("{}_{}.snapshot", label, now));

    engine
        .write_snapshot_to_file(snapshot_path.clone())
        .await
        .unwrap();

    let file = tokio::fs::File::open(&snapshot_path).await.unwrap();
    let meta = file.metadata().await.unwrap();
    assert_eq!(meta.len(), crate::types::HEADER_SIZE + 160);

    let _ = tokio::fs::remove_file(&snapshot_path).await;
}

#[tokio::test]
async fn test_purge_all_data() {
    let label = "test_purge_all_data";
    let engine = Orby::builder(label)
        .ring_buffer_lane_item_count(10)
        .ring_buffer_lane_count(1)
        .logic_mode(LogicMode::RingBuffer)
        .build()
        .await
        .unwrap();

    engine.insert_batch(&[[100u128], [200u128]]).await.unwrap();
    assert_eq!(engine.len(), 2);

    engine
        .purge_all_data(vec![[300u128], [400u128], [500u128]])
        .await
        .unwrap();
    assert_eq!(engine.len(), 3);
    assert_eq!(engine.get_at(0).unwrap()[0], 500);
    assert_eq!(engine.get_at(2).unwrap()[0], 300);

    let results = engine.query_raw(|row| row[0].as_u128() == 100, 10);
    assert_eq!(results.len(), 0);

    engine
        .purge_all_data(Vec::<[u128; 1]>::new())
        .await
        .unwrap();
    assert_eq!(engine.len(), 0);
}

#[tokio::test]
async fn test_insert_lane_batch() {
    let label = "test_lane_batch";
    let engine = Orby::builder(label)
        .ring_buffer_lane_item_count(10)
        .ring_buffer_lane_count(3)
        .build()
        .await
        .unwrap();

    // 1. insert_lane_batch でレーン 1 に 5 件書き込み
    let values = vec![100, 200, 300, 400, 500];
    engine.insert_lane_batch(1, &values).await.unwrap();

    assert_eq!(engine.len(), 5);

    // レーン 1 に値が入っているか、他のレーンが 0 かチェック
    for i in 0..5 {
        let row = engine.get_at(4 - i).unwrap(); // get_at(0) は最新なので index 4
        assert_eq!(row[1], values[i]);
        assert_eq!(row[0], 0);
        assert_eq!(row[2], 0);
    }

    // 2. ラップアラウンドのテスト
    let values2 = vec![600, 700, 800, 900, 1000, 1100, 1200]; // 合計 5+7=12 件（キャパ10を超える）
    engine.insert_lane_batch(0, &values2).await.unwrap();

    assert_eq!(engine.len(), 10);
    // 最新は 1200
    assert_eq!(engine.get_at(0).unwrap()[0], 1200);
}

#[tokio::test]
async fn test_vault_autoload() {
    let label = "test_vault_autoload";
    let db_path = std::env::temp_dir().join(format!("orby_vault_{}", label));
    if db_path.exists() {
        let _ = std::fs::remove_dir_all(&db_path);
    }

    // 1. 初回起動とデータ投入
    {
        let engine = Orby::builder(label)
            .ring_buffer_lane_item_count(10)
            .ring_buffer_lane_count(2)
            .with_storage(crate::types::SaveMode::Vault(Some(
                db_path.parent().unwrap().to_path_buf(),
            )))
            .build()
            .await
            .unwrap();

        engine
            .insert_batch(&[[101, 201], [102, 202]])
            .await
            .unwrap();
    } // engine is dropped here

    // 2. 再起動とオートロード
    {
        let engine = Orby::builder(label)
            .ring_buffer_lane_item_count(10)
            .ring_buffer_lane_count(2)
            .with_storage(crate::types::SaveMode::Vault(Some(
                db_path.parent().unwrap().to_path_buf(),
            )))
            .autoload(true)
            .build()
            .await
            .unwrap();

        assert_eq!(engine.len(), 2);
        let items = engine.take(2);
        assert_eq!(items[0][0], 102);
        assert_eq!(items[0][1], 202);
        assert_eq!(items[1][0], 101);
        assert_eq!(items[1][1], 201);
    }

    let _ = std::fs::remove_dir_all(&db_path);
}

#[tokio::test]
async fn test_vault_config_mismatch() {
    let label = "test_vault_mismatch";
    let db_path = std::env::temp_dir().join(format!("orby_vault_{}", label));
    if db_path.exists() {
        let _ = std::fs::remove_dir_all(&db_path);
    }

    // 1. 2レーンで作成
    {
        let _engine = Orby::builder(label)
            .ring_buffer_lane_item_count(10)
            .ring_buffer_lane_count(2)
            .with_storage(crate::types::SaveMode::Vault(Some(
                db_path.parent().unwrap().to_path_buf(),
            )))
            .build()
            .await
            .unwrap();
    }

    // 2. 3レーンで起動しようとするとエラー
    {
        let result = Orby::builder(label)
            .ring_buffer_lane_item_count(10)
            .ring_buffer_lane_count(3) // mismatch
            .with_storage(crate::types::SaveMode::Vault(Some(
                db_path.parent().unwrap().to_path_buf(),
            )))
            .autoload(true)
            .build()
            .await;

        assert!(result.is_err());
        match result.err().unwrap() {
            OrbyError::ConfigMismatch { .. } => {}
            e => panic!("Expected ConfigMismatch, got {:?}", e),
        }
    }

    let _ = std::fs::remove_dir_all(&db_path);
}
