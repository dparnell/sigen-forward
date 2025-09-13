use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use env_logger::{Builder, Env};
use log::{debug, error, info, trace, warn};
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use tokio::signal;
use tokio::time::sleep;

#[derive(Parser, Debug, Clone)]
#[command(name = "sigen-forward", about = "Forward MQTT messages to InfluxDB when topic matches a regex.")]
struct Cli {
    /// MQTT broker host (without scheme)
    #[arg(long, env = "MQTT_HOST", default_value = "localhost")]
    mqtt_host: String,

    /// MQTT broker port
    #[arg(long, env = "MQTT_PORT", default_value_t = 1883)]
    mqtt_port: u16,

    /// MQTT client id
    #[arg(long, env = "MQTT_CLIENT_ID", default_value = "sigen-forward")]
    mqtt_client_id: String,

    /// MQTT username (optional)
    #[arg(long, env = "MQTT_USERNAME")]
    mqtt_username: Option<String>,

    /// MQTT password (optional)
    #[arg(long, env = "MQTT_PASSWORD")]
    mqtt_password: Option<String>,

    /// MQTT subscription topic filter (supports wildcards like sensors/#)
    #[arg(long, env = "MQTT_SUB", default_value = "#")]
    mqtt_sub: String,

    /// Regex to match the MQTT topic. Only matching topics are forwarded.
    #[arg(long, env = "TOPIC_REGEX", default_value = ".*")]
    topic_regex: String,

    /// InfluxDB write URL (e.g., http://localhost:8086/api/v2/write?org=myorg&bucket=mybucket&precision=ns)
    #[arg(long, env = "INFLUX_URL")]
    influx_url: String,

    /// InfluxDB API token (for Authorization header). If empty, no auth header is sent.
    #[arg(long, env = "INFLUX_TOKEN")]
    influx_token: Option<String>,

    /// QoS for subscription (0,1,2)
    #[arg(long, env = "MQTT_QOS", default_value_t = 0)]
    mqtt_qos: u8,

    /// Keepalive seconds for MQTT
    #[arg(long, env = "MQTT_KEEPALIVE", default_value_t = 30)]
    mqtt_keepalive: u64,

    /// Maximum HTTP retry attempts when posting to InfluxDB
    #[arg(long, env = "HTTP_RETRIES", default_value_t = 3)]
    http_retries: usize,

    /// Base delay in milliseconds for HTTP retry backoff
    #[arg(long, env = "HTTP_RETRY_BACKOFF_MS", default_value_t = 250)]
    http_retry_backoff_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cli = Cli::parse();

    let topic_re = Regex::new(&cli.topic_regex)
        .with_context(|| format!("Invalid topic regex: {}", &cli.topic_regex))?;

    let (mqtt, mut eventloop) = connect_mqtt(&cli)?;
    let qos = match cli.mqtt_qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    };

    mqtt.subscribe(cli.mqtt_sub.clone(), qos)
        .await
        .with_context(|| format!("Subscribing to {}", &cli.mqtt_sub))?;
    info!("Subscribed to {}", &cli.mqtt_sub);

    let http = build_http_client(&cli)?;

    info!("Forwarding to InfluxDB: {}", &cli.influx_url);

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                info!("Shutdown requested (Ctrl+C). Exiting...");
                break;
            }
            ev = eventloop.poll() => {
                match ev {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        let topic = p.topic.clone();
                        let payload = p.payload;
                        if let Some(caps) = topic_re.captures(&topic) {
                            // Determine measurement: first capture group if present, otherwise the full match
                            let measurement_raw = if caps.len() > 1 {
                                caps.get(1).map(|m| m.as_str()).unwrap_or("")
                            } else {
                                caps.get(0).map(|m| m.as_str()).unwrap_or("")
                            };
                            let measurement = if measurement_raw.is_empty() { "mqtt" } else { measurement_raw };

                            // Build InfluxDB line protocol: <measurement> value=<parsed_payload>
                            let field_value = format_field_value(&payload);
                            let line = format!("{} value={}\n", escape_measurement(measurement), field_value);

                            if let Err(e) = forward_to_influx(&http, &cli.influx_url, cli.influx_token.as_deref(), line.as_bytes(), cli.http_retries, cli.http_retry_backoff_ms).await {
                                error!("Failed to forward message from topic '{}': {:#}", topic, e);
                            } else {
                                debug!("Forwarded message from topic '{}' to InfluxDB", topic);
                            }
                        } else {
                            trace!("Topic '{}' did not match regex", topic);
                        }
                    }
                    Ok(Event::Outgoing(_)) => {
                        // We can ignore outgoing events for now.
                    }
                    Ok(other) => {
                        // Other events like pings, acks, etc.
                        // warn!("MQTT event: {:?}", other);
                        let _ = other; // silence
                    }
                    Err(e) => {
                        warn!("MQTT event loop error: {}. Will retry after short delay.", e);
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

fn init_logger() {
    let env = Env::default().filter_or("SIGEN_FORWARD_LOG", "info");
    Builder::from_env(env).init();
}

fn connect_mqtt(cli: &Cli) -> Result<(AsyncClient, EventLoop)> {
    let mut mqttoptions = MqttOptions::new(&cli.mqtt_client_id, &cli.mqtt_host, cli.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(cli.mqtt_keepalive));
    if let (Some(u), Some(p)) = (&cli.mqtt_username, &cli.mqtt_password) {
        mqttoptions.set_credentials(u, p);
    } else if let Some(u) = &cli.mqtt_username {
        mqttoptions.set_credentials(u, "");
    }
    // Note: TLS not configured; use a TLS-enabled port or extend as needed.

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    Ok((client, eventloop))
}

fn build_http_client(_cli: &Cli) -> Result<reqwest::Client> {
    let client = reqwest::Client::builder()
        .user_agent("sigen-forward/0.1")
        .build()?;
    Ok(client)
}

async fn forward_to_influx(
    http: &reqwest::Client,
    influx_url: &str,
    influx_token: Option<&str>,
    payload: &[u8],
    max_retries: usize,
    backoff_ms: u64,
) -> Result<()> {
    // We assume payload is already in InfluxDB line protocol.
    // Content-Type can be text/plain.
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    if let Some(token) = influx_token {
        let value = format!("Token {}", token);
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&value)?);
    }

    let mut attempt = 0;
    loop {
        attempt += 1;
        let res = http
            .post(influx_url)
            .headers(headers.clone())
            .body(payload.to_vec())
            .send()
            .await;

        match res {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(());
                } else {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    if attempt > max_retries || !status.is_server_error() {
                        anyhow::bail!("Influx write failed with status {}: {}", status, text);
                    } else {
                        warn!("Influx write attempt {} failed with {}. Retrying...", attempt, status);
                    }
                }
            }
            Err(e) => {
                if attempt > max_retries {
                    return Err(e).context("Influx write HTTP error");
                }
                warn!("Influx write attempt {} errored: {}. Retrying...", attempt, e);
            }
        }

        sleep(Duration::from_millis(backoff_ms.saturating_mul(attempt as u64))).await;
    }
}


// Helpers to build InfluxDB line protocol entries
fn escape_measurement(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        match ch {
            ' ' => { out.push('\\'); out.push(' '); }
            ',' => { out.push('\\'); out.push(','); }
            '\t' => { out.push('\\'); out.push(' '); }
            _ => out.push(ch),
        }
    }
    out
}

fn escape_string_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => { out.push('\\'); out.push('"'); }
            '\\' => { out.push('\\'); out.push('\\'); }
            '\n' => { out.push('\\'); out.push('n'); }
            '\r' => { out.push('\\'); out.push('r'); }
            '\t' => { out.push('\\'); out.push('t'); }
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn format_field_value(payload: &[u8]) -> String {
    if let Ok(s) = std::str::from_utf8(payload) {
        let s_trim = s.trim();
        if s_trim.eq_ignore_ascii_case("true") {
            return "true".to_string();
        }
        if s_trim.eq_ignore_ascii_case("false") {
            return "false".to_string();
        }
        if let Ok(n) = s_trim.parse::<i64>() {
            return format!("{}i", n);
        }
        if let Ok(f) = s_trim.parse::<f64>() {
            return format!("{}", f);
        }
        escape_string_value(s_trim)
    } else {
        let lossy = String::from_utf8_lossy(payload);
        escape_string_value(lossy.trim())
    }
}
