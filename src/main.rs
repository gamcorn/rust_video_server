use serde::{Deserialize, Serialize};
use jsonrpc_core::{IoHandler, Params, Error};
use jsonrpc_http_server::ServerBuilder;
use once_cell::sync::Lazy;
use reqwest::Client;
use tempfile::NamedTempFile;
use std::io::Write;
use serde_yaml;
use sqlx::postgres::PgPoolOptions;
use tokio::{task, sync::mpsc};
use chrono;
use tracing::{info, error, debug, Level};
use tracing_subscriber::FmtSubscriber;
use std::process::Command;

static CLIENT: Lazy<Client> = Lazy::new(Client::new);

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

async fn download_video_to_tempfile(url: &str) -> Result<NamedTempFile, String> {
    info!("Downloading video from URL: {}", url);
    let response = CLIENT.get(url).send().await.map_err(|e| {
        error!("HTTP request failed: {}", e);
        format!("HTTP request error: {}", e)
    })?;

    if !response.status().is_success() {
        error!("Failed to download video: HTTP {}", response.status());
        return Err(format!("Failed to download video: HTTP {}", response.status()));
    }

    let bytes = response.bytes().await.map_err(|e| {
        error!("Failed to read response bytes: {}", e);
        format!("Failed getting bytes: {}", e)
    })?;

    let mut tempfile = NamedTempFile::new().map_err(|e| {
        error!("Failed to create temp file: {}", e);
        format!("Tempfile error: {}", e)
    })?;

    tempfile.write_all(&bytes).map_err(|e| {
        error!("Failed writing to temp file: {}", e);
        format!("Failed writing content: {}", e)
    })?;

    info!("Video downloaded to temporary file");
    Ok(tempfile)
}

fn probe_video_dimensions(file_path: &str) -> Result<(u32, u32), String> {
    debug!("Probing video dimensions for file: {}", file_path);

    let output = Command::new("mediainfo")
        .arg("--Inform=Video;%Width%x%Height%")
        .arg(file_path)
        .output()
        .map_err(|e| format!("Failed to execute mediainfo: {}", e))?;

    if !output.status.success() {
        return Err("mediainfo command failed".to_string());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let dims: Vec<&str> = stdout.trim().split('x').collect();

    if dims.len() != 2 {
        return Err("Unexpected mediainfo output".to_string());
    }

    let width = dims[0].parse::<u32>().map_err(|_| "Failed to parse width".to_string())?;
    let height = dims[1].parse::<u32>().map_err(|_| "Failed to parse height".to_string())?;

    debug!("Parsed dimensions: width={}, height={}", width, height);
    Ok((width, height))
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

    let db_url = format!("postgres://{}:{}@{}/{}",
                         config.database.user, config.database.password, config.database.url, config.database.dbname);

    let db_pool = PgPoolOptions::new()
        .max_connections(config.app_conf.max_pg_pool_conn)
        .connect(&db_url)
        .await?;

    info!("Database connection established");

    let (tx, mut rx) = mpsc::channel::<(VideoRequest, (u32, u32))>(100);

    let db_pool_clone = db_pool.clone();
    tokio::spawn(async move {
        while let Some((parsed, dimensions)) = rx.recv().await {
            let request_date = chrono::DateTime::parse_from_rfc3339(&parsed.date)
                .map(|dt| dt.naive_utc())
                .unwrap_or_else(|_| chrono::NaiveDateTime::from_timestamp(0, 0));

            if let Err(e) = sqlx::query!(
                "INSERT INTO video_requests (identifier, file_name, request_date, width, height) VALUES ($1, $2, $3, $4, $5)",
                parsed.identifier,
                parsed.file_name,
                request_date,
                dimensions.0 as i32,
                dimensions.1 as i32
            ).execute(&db_pool_clone).await {
                error!("Database insert failed: {:?}", e);
            } else {
                debug!("Database insert successful for identifier: {}", parsed.identifier);
            }
        }
    });

    let mut io = IoHandler::new();

    {
        let tx = tx.clone();
        io.add_method("getVideoDimensions", move |params: Params| {
            let tx = tx.clone();
            async move {
                info!("Received getVideoDimensions request");
                let parsed: VideoRequest = params.parse()
                    .map_err(|e| {
                        error!("Invalid request params: {}", e);
                        Error::invalid_params(format!("Invalid params: {}", e))
                    })?;

                let tempfile = download_video_to_tempfile(&parsed.url).await
                    .map_err(|e| {
                        error!("Download error: {}", e);
                        Error { code: jsonrpc_core::ErrorCode::InternalError, message: e.clone(), data: Some(e.into()) }
                    })?;

                let path_str = tempfile.path().to_str().unwrap().to_owned();
                let dimensions = task::spawn_blocking(move || probe_video_dimensions(&path_str))
                    .await
                    .map_err(|e| {
                        error!("Join error: {:?}", e);
                        Error { code: jsonrpc_core::ErrorCode::InternalError, message: format!("Join Error: {:?}", e), data: None }
                    })?
                    .map_err(|e| {
                        error!("Probing error: {}", e);
                        Error { code: jsonrpc_core::ErrorCode::InternalError, message: e.clone(), data: Some(e.into()) }
                    })?;

                if tx.send((parsed.clone(), dimensions)).await.is_err() {
                    error!("Failed to queue database insert");
                }

                info!("Returning dimensions: width={}, height={}", dimensions.0, dimensions.1);

                Ok(serde_json::to_value(VideoDimensions { width: dimensions.0, height: dimensions.1 }).unwrap())
            }
        });
    }

    info!("Starting RPC server at http://0.0.0.0:3030");
    let server = ServerBuilder::new(io)
        .threads(config.app_conf.num_threads)
        .start_http(&"0.0.0.0:3030".parse().unwrap())
        .expect("Unable to start RPC server");

    println!("Server listening at http://0.0.0.0:3030");
    server.wait();

    Ok(())
}
