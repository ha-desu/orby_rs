use super::*;
use crate::row::PulseCellPack;

#[tokio::test]
async fn test_insert() {
    let label = "test_insert";
    let engine = Orby::new(label, 10, 2, SaveMode::MemoryOnly, LogicMode::Ring)
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
    let engine = Orby::new(label, 10, 2, SaveMode::MemoryOnly, LogicMode::Ring)
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
    let engine = Orby::new(label, 10, 2, SaveMode::MemoryOnly, LogicMode::Ring)
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
async fn test_stride_alignment() {
    let label = "test_alignment";
    let engine = Orby::new(label, 10, 2, SaveMode::MemoryOnly, LogicMode::Ring)
        .await
        .unwrap();

    let padded_dim = engine.inner.read().stride;
    assert_eq!(
        padded_dim, 4,
        "Dimension 2 should result in stride 4 for 64-byte alignment"
    );
}

#[tokio::test]
async fn test_update_and_upsert() {
    let label = "test_update";
    let engine = Orby::builder(label)
        .capacity(10)
        .dimension(2)
        .logic_mode(LogicMode::Ring)
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
        .capacity(5)
        .dimension(1)
        .logic_mode(LogicMode::Ring)
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
        .capacity(5)
        .dimension(1)
        .logic_mode(LogicMode::Ring)
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
async fn test_mirror_persistence() {
    use std::io::SeekFrom;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let temp_dir = std::env::temp_dir();
    let label = "test_mirror";
    let mirror_path = temp_dir.join(format!("{}.orby", label));

    if mirror_path.exists() {
        let _ = tokio::fs::remove_file(&mirror_path).await;
    }

    let engine = Orby::builder(label)
        .capacity(10)
        .dimension(2)
        .logic_mode(LogicMode::Ring)
        .with_storage(SaveMode::Sync(Some(temp_dir.clone())))
        .build()
        .await
        .unwrap();

    let mut file = tokio::fs::File::open(&mirror_path).await.unwrap();
    let meta = file.metadata().await.unwrap();
    assert_eq!(meta.len(), crate::types::HEADER_SIZE + 640);

    let val1 = 0xA_u128;
    let val2 = 0xB_u128;
    engine.insert_batch(&[[val1, val2]]).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut buffer = vec![0u8; 16];
    file.seek(SeekFrom::Start(crate::types::HEADER_SIZE))
        .await
        .unwrap();
    file.read_exact(&mut buffer).await.unwrap();
    let stored_val1 = u128::from_le_bytes(buffer.as_slice().try_into().unwrap());
    assert_eq!(stored_val1, val1);

    file.read_exact(&mut buffer).await.unwrap();
    let stored_val2 = u128::from_le_bytes(buffer.as_slice().try_into().unwrap());
    assert_eq!(stored_val2, val2);

    let mut padding = vec![0u8; 32];
    file.read_exact(&mut padding).await.unwrap();
    assert!(padding.iter().all(|&b| b == 0));

    let _ = tokio::fs::remove_file(&mirror_path).await;
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
        .capacity(10)
        .dimension(2)
        .logic_mode(LogicMode::Ring)
        .with_storage(SaveMode::Direct(Some(temp_dir.clone())))
        .build()
        .await
        .unwrap();

    assert!(engine.inner.read().buffer.is_empty());

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

    // Verify via query_iter (should also read from file via reused handle)
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
        .capacity(10)
        .dimension(2)
        .logic_mode(LogicMode::Ring)
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

    engine.write_snapshot_to_file(&snapshot_path).unwrap();

    let file = tokio::fs::File::open(&snapshot_path).await.unwrap();
    let meta = file.metadata().await.unwrap();
    assert_eq!(meta.len(), crate::types::HEADER_SIZE + 640);

    let _ = tokio::fs::remove_file(&snapshot_path).await;
}

#[tokio::test]
async fn test_purge_all_data() {
    let label = "test_purge_all_data";
    let engine = Orby::builder(label)
        .capacity(10)
        .dimension(1)
        .logic_mode(LogicMode::Ring)
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
async fn test_storage_restoration() {
    let temp_dir = std::env::temp_dir();
    let label = "test_restore";
    let path = temp_dir.join(format!("{}.orby", label));

    if path.exists() {
        let _ = tokio::fs::remove_file(&path).await;
    }

    {
        let engine = Orby::builder(label)
            .capacity(10)
            .dimension(2)
            .with_storage(SaveMode::Sync(Some(temp_dir.clone())))
            .build()
            .await
            .unwrap();

        engine
            .insert_batch(&[[100, 200], [300, 400]])
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    }

    {
        let engine = Orby::builder(label)
            .capacity(10)
            .dimension(2)
            .with_storage(SaveMode::Sync(Some(temp_dir.clone())))
            .from_file(&path)
            .build()
            .await
            .unwrap();

        assert_eq!(engine.len(), 2);
        assert_eq!(engine.get_at(0).unwrap()[0], 300);
        assert_eq!(engine.get_at(1).unwrap()[0], 100);
    }

    let _ = tokio::fs::remove_file(&path).await;
}
