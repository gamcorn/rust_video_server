use serde::{Deserialize, Serialize};
use jsonrpc_core::{IoHandler, Params, Error};
use jsonrpc_http_server::ServerBuilder;
use reqwest::Client;
use tempfile::NamedTempFile;
use std::io::Write;
use ffmpeg_next as ffmpeg;
use serde_yaml;
use sqlx::postgres::PgPoolOptions;
use tokio::task;
use chrono;
use log::{info, debug, error};
use env_logger::Env;

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
}

#[derive(Debug, Deserialize)]
struct Settings {
    database: DatabaseConfig,
    app_conf: AppConfig,
}


#[derive(Deserialize, Debug)]
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
    info!("Loading configuration file: {}", path);
    let content = tokio::fs::read_to_string(path).await?;
    debug!("Config file content loaded");
    let cfg: Settings = serde_yaml::from_str(&content)?;
    info!("Config file parsed successfully");
    Ok(cfg)
}

async fn download_video_to_tempfile(url: &str) -> Result<NamedTempFile, String> {
    info!("Starting download for video from URL {}", url);
    let response = Client::new()
        .get(url)
        .send()
        .await
        .map_err(|e| {
            error!("HTTP request failed: {}", e);
            format!("HTTP request error: {}", e)
        })?;

    if !response.status().is_success() {
        error!("Download failed with HTTP status code: {}", response.status());
        return Err(format!("Failed to download video: HTTP {}", response.status()));
    }

    let bytes = response.bytes().await
        .map_err(|e| {
            error!("Error reading response bytes: {}", e);
            format!("Failed getting bytes: {}", e)
        })?;

    let mut tempfile = NamedTempFile::new().map_err(|e| {
        error!("Failed creating temp file: {}", e);
        format!("Tempfile error: {}", e)
    })?;

    tempfile.write_all(&bytes).map_err(|e| {
        error!("Error writing video content to tempfile: {}", e);
        format!("Failed writing content: {}", e)
    })?;

    info!("Video downloaded successfully to temporary file {:?}", tempfile.path());
    Ok(tempfile)
}

fn probe_video_dimensions(file_path: &str) -> Result<(u32, u32), String> {
    info!("Analyzing video file dimensions for file: {}", file_path);
    ffmpeg::init().map_err(|e| {
        error!("FFmpeg initialization error: {}", e);
        format!("FFmpeg init error: {}", e)
    })?;

    let ictx = ffmpeg::format::input(&file_path)
        .map_err(|e| {
            error!("FFmpeg cannot open input file: {}", e);
            format!("FFmpeg input error: {}", e)
        })?;

    debug!("Video file opened successfully.");

    let stream = ictx.streams()
        .best(ffmpeg::media::Type::Video)
        .ok_or_else(|| {
            error!("No video stream found in file");
            "No video stream detected".to_string()
        })?;

    let codec_ctx = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
        .map_err(|e| {
            error!("Failed to obtain codec context: {}", e);
            format!("Codec context error: {}", e)
        })?
        .decoder()
        .video()
        .map_err(|e| {
            error!("Failed to obtain video decoder context: {}", e);
            format!("Decoder error: {}", e)
        })?;

    let (w, h) = (codec_ctx.width(), codec_ctx.height());
    info!("Video dimensions parsed: width={} height={}", w, h);
    Ok((w, h))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config("./config.yml").await?;

    std::env::set_var("RUST_LOG", &config.app_conf.log_level);
    env_logger::init_from_env(Env::default().default_filter_or(&config.app_conf.log_level));
    info!("Configuration loaded successfully: {:?}", config);

    debug!("Database URL being used: {:?}", config.database.url);

    let db_url = format!("postgres://{}:{}@{}/{}",
                         config.database.user,
                         config.database.password,
                         config.database.url,
                         config.database.dbname
    );

    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await?;

    let mut io = IoHandler::new();

    {
        let db_pool = db_pool.clone();
        io.add_method("getVideoDimensions", move |params: Params| {
            let db_pool = db_pool.clone();

            async move {
                info!("Received getVideoDimensions request");
                debug!("Parameters received: {:?}", params);

                let parsed: VideoRequest = params.parse()
                    .map_err(|e| {
                        error!("Invalid params: {}", e);
                        Error::invalid_params(format!("Invalid params: {}", e))
                    })?;
                debug!("Parsed request: {:?}", parsed);
                debug!("Received date: {}", parsed.date);

                let tempfile = download_video_to_tempfile(&parsed.url).await
                    .map_err(|e| {
                        error!("Failed to download video: {}", e);
                        Error {
                            code: jsonrpc_core::ErrorCode::InternalError,
                            message: "Download Error".to_string(),
                            data: Some(format!("Download Error: {}", e).into()),
                        }
                    })?;

                let path_str = tempfile.path().to_str().unwrap().to_owned();
                debug!("Temporary video file path: {}", path_str);

                let dimensions = task::spawn_blocking(move || probe_video_dimensions(&path_str))
                    .await
                    .map_err(|e| {
                        error!("Join Error: {:?}", e);
                        Error {
                            code: jsonrpc_core::ErrorCode::InternalError,
                            message: "Join Error".to_string(),
                            data: Some(format!("Join Error: {:?}", e).into()),
                        }
                    })?
                    .map_err(|e| {
                        error!("Probe Error: {}", e);
                        Error {
                            code: jsonrpc_core::ErrorCode::InternalError,
                            message: "Probe Error".to_string(),
                            data: Some(format!("Probe Error: {}", e).into()),
                        }
                    })?;

                debug!("Dimensions probed successfully: {:?}", dimensions);

                let request_date = chrono::DateTime::parse_from_rfc3339(&parsed.date)
                    .map_err(|e| {
                        error!("Invalid date format: {}", e);
                        jsonrpc_core::Error {
                            code: jsonrpc_core::ErrorCode::InvalidParams,
                            message: "Invalid date format".to_string(),
                            data: Some(format!("Date parse error: {}", e).into()),
                        }
                    })?
                    .naive_utc();

                info!("Inserting request data into database...");
                debug!(
                "Insert parameters: identifier={}, file_name={}, request_date={}, width={}, height={}",
                parsed.identifier, parsed.file_name, request_date, dimensions.0, dimensions.1
                );

                sqlx::query!(
                        "INSERT INTO video_requests (
                            identifier, file_name, request_date, width, height
                            ) VALUES ($1, $2, $3, $4, $5)",
                        parsed.identifier,
                        parsed.file_name,
                        request_date,
                        dimensions.0 as i32,
                        dimensions.1 as i32
                    )
                    .execute(&db_pool)
                    .await
                    .map_err(|e| jsonrpc_core::Error {
                        code: jsonrpc_core::ErrorCode::InternalError,
                        message: "Database Insert Error".to_string(),
                        data: Some(format!("Database error: {:?}", e).into())
                    })?;

                info!("Request handled successfully.");

                Ok(serde_json::to_value(VideoDimensions {
                    width: dimensions.0,
                    height: dimensions.1,
                }).unwrap())
            }
        });
    }

    info!("Starting RPC server at http://0.0.0.0:3030");
    let server = ServerBuilder::new(io)
        .threads(4)
        .start_http(&"0.0.0.0:3030".parse().unwrap())
        .expect("Unable to start RPC server");

    println!("Server listening at http://0.0.0.0:3030");
    server.wait();

    Ok(())
}