use std::sync::Arc;

use object_store::{path::Path, ObjectStore, ObjectMeta};
use futures::{ TryStreamExt, stream::FuturesOrdered, StreamExt};

/// Object lock for some S3 path.
/// 
/// The intuition is that each concurrent lock acts as a person in a queue. At
/// any given time, we just need to do two things:
/// 
/// 1. Check if the person in front of us is still in line (if there is one)
/// 3. Show to the folks behind us we are still in line
/// 
/// Each lock creates two temporary objects:
/// * Request: Any empty object that is used to indicate we are in the queue.
///   The last modified time of this object is used to determine our position
///   (with the value of our key as a tie-breaker).
/// * Heartbeat: An object we continuously update to indicate we are still in
///   the queue and haven't crashed.
/// 
/// After creating both objects, we check the heartbeat of the locks in front
/// of us. If it is expired, we can move up in the queue. Once we reach the
/// first position, we have the lock. If at any time we find the target object
/// now exists, then we have been beaten and we can release our lock.
struct ObjectLock {
    store: Arc<dyn ObjectStore>,
    request_path: Path,
    heartbeat_path: Path,
    heartbeat_handle: tokio::task::JoinHandle<()>,
}

impl ObjectLock {
    async fn acquire(store: Arc<dyn ObjectStore>, path: Path) -> Result<Self, String> {
        let lock_id = uuid::Uuid::new_v4();
        log::info!("{}: New lock", &lock_id);

        let tmp_path = Path::from(path.to_string() + ".tmp");

        // Create a request object
        let request_path = tmp_path.child("lock-request").child(lock_id.to_string());
        store.put(&request_path, vec![].into()).await
            .map_err(|e| format!("Failed to create object: {}", e))?;

        log::info!("{}: Wrote request object", &lock_id);

        // Setup heartbeat
        let heartbeat_path = tmp_path.child("lock-heartbeat").child(lock_id.to_string());
        let heartbeat_path2 = heartbeat_path.clone();
        let store2 = store.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let heartbeat_interval = std::time::Duration::from_secs(1);
            let mut interval = tokio::time::interval(heartbeat_interval);
            loop {
                let current_timestamp_milliseconds = chrono::Utc::now().timestamp_millis() + (2 * heartbeat_interval.as_millis() as i64);
                let encoded_timestamp = current_timestamp_milliseconds.to_be_bytes().to_vec();
                store2.put(&heartbeat_path2, encoded_timestamp.into()).await.unwrap();
                interval.tick().await;
                // TODO: handle shutdown
            }
        });

        log::info!("{}: Wrote heartbeat object", &lock_id);

        // sleep for consistenty
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // Check our position in the queue
        let queue_prefix = tmp_path.child("lock-request");
        let mut queue = store.list(Some(&queue_prefix)).await
            .map_err(|e| format!("Failed to list objects: {}", e))?
            .map_err(|e| format!("Failed to list objects: {}", e))
            .try_collect::<Vec<ObjectMeta>>()
            .await?;
        queue.sort_by_key(|p| p.last_modified);

        let heartbeat_queue = queue.iter()
            .map(|meta| {
                Path::from(meta.location.to_string().replace("/lock-request/", "/lock-heartbeat/"))
            })
            .collect::<Vec<Path>>();

        let mut position = queue.iter().position(|p| p.location == request_path)
            .ok_or_else(|| format!("Failed to find request object in queue"))?;
        
        // Main loop:
        // If the object at position - 1 is expired, we can move up positions.
        // If the target object is found, then we have been beaten.
        // Once position == 0, then we have the lock.
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        while position > 0 {
            // Wait for either path to be written, or for the heartbeat in front
            // of us to fail.
            interval.tick().await;
            loop {
                let heartbeat = store.get(&heartbeat_queue[position - 1]).await
                .map_err(|e| format!("Failed to get object: {}", e))?
                .bytes().await.map_err(|e| format!("Failed to get object: {}", e))?;
                let expiration = i64::from_be_bytes([heartbeat[0], heartbeat[1], heartbeat[2], heartbeat[3], heartbeat[4], heartbeat[5], heartbeat[6], heartbeat[7]]);
                if expiration < chrono::Utc::now().timestamp_millis() {
                    position -= 1;
                    if position == 0 {
                        break;
                    }
                } else {
                    break;
                }
            }

            if let Err(object_store::Error::NotFound { .. }) = store.head(&path).await {
                continue;
            } else {
                // store.delete(&request_path).await.unwrap();
                // store.delete(&heartbeat_path).await.unwrap();
                return Err(format!("Object committed by another process."));
            }
        }
        
        Ok(ObjectLock {
            store: store,
            request_path: request_path,
            heartbeat_path: heartbeat_path,
            heartbeat_handle,
        })
    }

    async fn release(self) {
        self.heartbeat_handle.abort();
        self.store.delete(&self.request_path).await.unwrap();
        self.store.delete(&self.heartbeat_path).await.unwrap();
    }
}


#[tokio::main]
async fn main() {
    env_logger::init();

    let num_tasks = 10;

    let bucket = std::env::var("BUCKET").expect("BUCKET must be set");
    let endpoint = std::env::var("AWS_ENDPOINT_OVERRIDE").expect("AWS_ENDPOINT_OVERRIDE must be set");
    let access_key = std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
    let store = Arc::new(object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_allow_http(true)
        .with_endpoint(endpoint)
        .with_region("us-east-1")
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .build()
        .unwrap());

    let path = Path::from("test-lock");

    let mut tasks = (0..num_tasks).map(|i| {
        let store = store.clone();
        let path = path.clone();
        tokio::spawn(async move {
            log::info!("{}: Started", i);
            let lock = ObjectLock::acquire(store.clone(), path.clone()).await
                .expect("Failed to acquire lock");
            log::info!("{}: Acquired lock", i);
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;

            // with probability 0.2, return early
            if rand::random::<f64>() > 0.4 {
                log::info!("{}: Writing object", i);
                store.put(&path, "hello world".into()).await
                    .expect("Failed to write object");
            } else {
                log::info!("{}: Not writing object", i);
            }
            
            lock.release().await;
            log::info!("{}: Released lock", i);
        })
    })
    .collect::<FuturesOrdered<_>>();

    while let Some(_) = tasks.next().await {}
}
