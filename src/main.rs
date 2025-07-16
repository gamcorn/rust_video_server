use serde::{Deserialize, Serialize};
use jsonrpc_core::{Params, Error};
use jsonrpc_http_server::ServerBuilder;
use reqwest::Client;
use serde_yaml;
use sqlx::postgres::PgPoolOptions;
use tokio::{sync::mpsc};
use chrono;
use tracing::{info, error, debug, Level};
use tracing_subscriber::FmtSubscriber;
use std::convert::TryInto;
use std::sync::Arc;
use chrono::TimeZone;
use memchr::memmem;

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    url: String,
    user: String,
    password: String,
    dbname: String,
}

#[derive(Debug, Deserialize)]
struct AppConfig {
    log_level: String,
    num_threads: usize,
    max_pg_pool_conn: u32,
}

#[derive(Debug, Deserialize)]
struct Settings {
    database: DatabaseConfig,
    app_conf: AppConfig,
}

#[derive(Deserialize, Debug, Clone)]
struct VideoRequest {
    file_name: String,
    date: String,
    identifier: String,
    url: String,
}

#[derive(Serialize)]
struct VideoDimensions {
    width: u32,
    height: u32,
}

async fn load_config(path: &str) -> Result<Settings, Box<dyn std::error::Error>> {
    info!("Loading configuration from {}", path);
    let content = tokio::fs::read_to_string(path).await?;
    let config: Settings = serde_yaml::from_str(&content)?;
    info!("Configuration loaded successfully");
    Ok(config)
}

async fn ensure_database_exists(db_config: &DatabaseConfig) -> Result<(), Box<dyn std::error::Error>> {
    let default_conn_str = format!("postgres://{}:{}@{}/postgres",
        db_config.user, db_config.password, db_config.url);
    let pool = PgPoolOptions::new().connect(&default_conn_str).await?;
    
    let row: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM pg_database WHERE datname = $1")
        .bind(&db_config.dbname)
        .fetch_optional(&pool)
        .await?;
    
    if row.is_none() {
        info!("Database '{}' does not exist. Creating...", db_config.dbname);
        let create_db_query = format!("CREATE DATABASE {}", db_config.dbname);
        sqlx::query(&create_db_query).execute(&pool).await?;
        info!("Database '{}' created successfully", db_config.dbname);
    } else {
        info!("Database '{}' already exists", db_config.dbname);
    }
    Ok(())
}

async fn ensure_table_exists(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    info!("Ensuring video_requests table exists...");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS video_requests (
            id SERIAL PRIMARY KEY,
            identifier TEXT NOT NULL,
            file_name TEXT NOT NULL,
            request_date TIMESTAMP NOT NULL,
            width INT NOT NULL,
            height INT NOT NULL
        )"
    ).execute(pool).await?;
    info!("Table video_requests is ready");
    Ok(())
}

async fn fetch_partial_video_data(client: &Client, url: &str, range: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let response = client.get(url)
        .header("Range", range)
        .send()
        .await?;
    let bytes = response.bytes().await?;
    Ok(bytes.to_vec())
}

fn get_video_dimensions_avi(data: &[u8]) -> Result<(u32, u32), Box<dyn std::error::Error>> {
    let needle = b"avih";
    if let Some(pos) = memmem::find(data, needle) {
        if data.len() < pos + 8 + 40 {
            return Err("Not enough data after avih header".into());
        }
        let header_start = pos + 8;
        let width = u32::from_le_bytes(data[header_start + 32..header_start + 36].try_into()?);
        let height = u32::from_le_bytes(data[header_start + 36..header_start + 40].try_into()?);
        if width == 0 || height == 0 {
            return Err("Invalid dimensions found in AVI header".into());
        }
        Ok((width, height))
    } else {
        Err("AVI header (avih) not found in data".into())
    }
}

fn parse_log_level(level: &str) -> Level {
    match level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config("./config.yml").await?;
    let log_level = parse_log_level(&config.app_conf.log_level);
    
    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");
    
    ensure_database_exists(&config.database).await?;
    
    let db_url = format!("postgres://{}:{}@{}/{}",
                         config.database.user, config.database.password, config.database.url, config.database.dbname);
    
    let db_pool = PgPoolOptions::new()
        .max_connections(config.app_conf.max_pg_pool_conn)
        .connect(&db_url)
        .await?;
    
    info!("Database connection established");
    
    ensure_table_exists(&db_pool).await?;
    
    let (tx, mut rx) = mpsc::channel::<(VideoRequest, (u32, u32))>(100);
    
    let db_pool_clone = Arc::new(db_pool.clone());
    let tx = Arc::new(tx.clone());
    tokio::spawn({
        let db_pool_clone = Arc::clone(&db_pool_clone);
        async move {
            while let Some((parsed, dimensions)) = rx.recv().await {
                let db_pool = Arc::clone(&db_pool_clone);
                tokio::spawn(async move {
                    let request_date = chrono::DateTime::parse_from_rfc3339(&parsed.date)
                        .map(|dt| dt.naive_utc())
                        .unwrap_or_else(|_| chrono::Utc.timestamp_opt(0, 0).single().unwrap().naive_utc());

                    let identifier_for_log = parsed.identifier.clone();

                    if let Err(e) = sqlx::query(
                        "INSERT INTO video_requests (identifier, file_name, request_date, width, height) VALUES ($1, $2, $3, $4, $5)"
                    )
                        .bind(parsed.identifier)
                        .bind(parsed.file_name)
                        .bind(request_date)
                        .bind(dimensions.0 as i32)
                        .bind(dimensions.1 as i32)
                        .execute(&*db_pool)
                        .await {
                        error!("Database insert failed: {:?}", e);
                    } else {
                        debug!("Database insert successful for identifier: {}", identifier_for_log);
                    }
                });
            }
        }
    });
    
    let client = Client::builder().build()?;
    let mut io = jsonrpc_core::IoHandler::new();
    
    let tx_for_method = Arc::new(tx.clone());
    io.add_method("getVideoDimensions", {
        let tx = Arc::clone(&tx_for_method);
        move |params: Params| {
            let tx = Arc::clone(&tx);
            let client = client.clone();
            Box::pin(async move {
                info!("Received getVideoDimensions request");
                let parsed: VideoRequest = params.parse().map_err(|e| {
                    error!("Invalid request params: {}", e);
                    Error::invalid_params(format!("Invalid params: {}", e))
                })?;
    
                let data = fetch_partial_video_data(&client, &parsed.url, "bytes=0-8191")
                    .await
                    .map_err(|e| {
                        error!("Fetching video failed with error: {}", e);
                        Error::internal_error()
                    })?;
    
                let dimensions = get_video_dimensions_avi(&data)
                    .map_err(|e| {
                        error!("Getting video dimensions failed with error: {}", e);
                        Error::internal_error()
                    })?;
    
                // Send to background task for DB insertion.
                if tx.send((parsed.clone(), dimensions)).await.is_err() {
                    error!("Failed to queue database insert");
                }
    
                info!("Returning dimensions: width={}, height={}", dimensions.0, dimensions.1);
                Ok(serde_json::to_value(VideoDimensions {
                    width: dimensions.0,
                    height: dimensions.1,
                }).unwrap())
            })
        }
    });
    
    info!("Starting RPC server at http://0.0.0.0:3030");
    let server = ServerBuilder::new(io)
        .threads(config.app_conf.num_threads)
        .start_http(&"0.0.0.0:3030".parse().unwrap())
        .expect("Unable to start RPC server");
    
    println!("Server listening at http://0.0.0.0:3030");
    server.wait();
    
    Ok(())
}