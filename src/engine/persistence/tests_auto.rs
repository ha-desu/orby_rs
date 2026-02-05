use crate::engine::Orby;
use crate::types::SaveMode;
use std::fs;
use std::path::PathBuf;

#[tokio::test]
async fn test_auto_restore_from_vault() {
    let temp_path = std::env::temp_dir().join("orby_test_auto_restore");
    let test_dir = temp_path.to_str().unwrap();
    let name = "test_ring_v2";
    let _sub_dir = PathBuf::from(test_dir).join(name);

    // 1. Clean up previous test data
    if PathBuf::from(test_dir).exists() {
        fs::remove_dir_all(test_dir).unwrap();
    }

    // 2. Create Orby and write data
    {
        let orby = Orby::builder(name)
            .ring_buffer_lane_item_count(100)
            .ring_buffer_lane_count(2)
            .with_storage(SaveMode::Vault(Some(test_dir.into())))
            .build()
            .await
            .expect("First build failed");

        let id = 12345u128;
        let data = 99999u128;
        orby.insert_batch(vec![vec![id, data]])
            .await
            .expect("Insert failed");

        // Persist
        orby.sleep().await.expect("Sleep failed");
    }

    // 3. Re-open Orby (Should auto-restore)
    {
        // Using default strict_check=true and autoload=true from builder
        let orby = Orby::builder(name)
            .ring_buffer_lane_item_count(100)
            .ring_buffer_lane_count(2)
            .with_storage(SaveMode::Vault(Some(test_dir.into())))
            .build()
            .await
            .expect("Re-build failed");

        // Verify data
        assert_eq!(orby.len(), 1, "Length mismatch after restore");
        let results = orby.query_raw(|row| row[0].as_u128() == 12345, 10);
        assert_eq!(results.len(), 1, "Query failed after restore");
        assert_eq!(results[0][1], 99999, "Data mismatch after restore");
    }

    // Clean up
    if PathBuf::from(test_dir).exists() {
        fs::remove_dir_all(test_dir).unwrap();
    }
}
