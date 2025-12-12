use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::payload::parse_header;
use crate::time_sync::now_unix_ns_estimate;
use crate::transport::{ConnectOptions, Engine, TransportBuilder, TransportMessage};
use anyhow::Result;
use flume;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

pub struct SubscriberConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub key_expr: String,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation/external snapshot support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
    // Test-only convenience: stop automatically after N seconds if provided
    pub test_stop_after_secs: Option<u64>,
}

pub async fn run_subscriber(config: SubscriberConfig) -> Result<()> {
    info!(
        engine = ?config.engine,
        key = %config.key_expr,
        endpoint = ?config.connect.params.get("endpoint"),
        "Starting subscriber"
    );

    // Initialize statistics early (use shared if provided)
    let stats = if let Some(s) = &config.shared_stats {
        s.clone()
    } else {
        Arc::new(Stats::new())
    };

    // Initialize Transport with optional retry
    stats.record_connection_attempt();
    let transport = match TransportBuilder::connect_with_retry(
        config.engine.clone(),
        config.connect.clone(),
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "Transport connect error");
            stats.record_connection_failure();
            // Return gracefully - the stats will reflect the failure
            return Ok(());
        }
    };
    info!(engine = ?config.engine, "Connected via transport");

    // Setup output writer (only when not aggregated/external)
    let mut output = if let Some(ref path) = config.output_file {
        Some(OutputWriter::new_csv(path.clone()).await?)
    } else if config.shared_stats.is_none() {
        Some(OutputWriter::new_stdout())
    } else {
        None
    };

    // Start snapshot task (only if not disabled)
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = Arc::clone(&stats);
        let interval_secs = config.snapshot_interval_secs;
        let mut out = output.take();
        Some(tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(interval_secs));
            loop {
                interval_timer.tick().await;
                let snapshot = stats_clone.snapshot().await;
                if let Some(ref mut o) = out {
                    let _ = o.write_snapshot(&snapshot).await;
                } else {
                    debug!(
                        received = snapshot.received_count,
                        errors = snapshot.error_count,
                        rate = format!("{:.2}", snapshot.interval_throughput()),
                        p99_ms = format!("{:.2}", snapshot.latency_ns_p99 as f64 / 1_000_000.0),
                        "Subscriber stats"
                    );
                }
            }
        }))
    } else {
        None
    };

    // Channel + worker to avoid per-message work in callback; send (recv_time, header_bytes)
    let (tx, rx) = flume::unbounded::<(u64, [u8; 24])>();
    let stats_worker = stats.clone();
    tokio::spawn(async move {
        let mut buf = Vec::with_capacity(1024);
        loop {
            // Block until at least 1 item
            let first = match rx.recv_async().await {
                Ok(v) => v,
                Err(_) => break,
            };
            buf.clear();
            buf.push(first);
            // Drain a small batch without awaiting to amortize locking
            while let Ok(v) = rx.try_recv() {
                buf.push(v);
                if buf.len() >= 1024 {
                    break;
                }
            }
            // Parse headers and compute latencies
            let mut lats = Vec::with_capacity(buf.len());
            for (recv_ns, hdr) in buf.drain(..) {
                if let Ok(h) = parse_header(&hdr) {
                    lats.push(recv_ns.saturating_sub(h.timestamp_ns));
                }
            }
            stats_worker.record_received_batch(&lats).await;
        }
    });

    // Subscribe via Transport with a handler
    let handler_tx = tx.clone();
    let subscription = transport
        .subscribe(
            &config.key_expr,
            Box::new(move |msg: TransportMessage| {
                // Minimal callback: copy 24-byte header and enqueue with receive timestamp
                let mut hdr = [0u8; 24];
                let bytes = msg.payload.as_cow();
                if bytes.len() >= 24 {
                    hdr.copy_from_slice(&bytes[..24]);
                    let recv = now_unix_ns_estimate();
                    let _ = handler_tx.try_send((recv, hdr));
                }
            }),
        )
        .await
        .map_err(|e| anyhow::Error::msg(format!("subscribe error: {}", e)))?;
    info!(key = %config.key_expr, "Subscribed to key expression");

    // Wait for Ctrl+C or optional test timeout; callbacks will keep updating stats
    if let Some(s) = config.test_stop_after_secs {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(s)) => {}
            _ = signal::ctrl_c() => {}
        }
    } else {
        signal::ctrl_c().await?;
    }
    info!("Ctrl+C received, stopping subscriber");

    // Final statistics
    let final_stats = stats.snapshot().await;
    info!(
        received = final_stats.received_count,
        errors = final_stats.error_count,
        avg_rate = format!("{:.2}", final_stats.total_throughput()),
        p50_ms = format!("{:.2}", final_stats.latency_ns_p50 as f64 / 1_000_000.0),
        p95_ms = format!("{:.2}", final_stats.latency_ns_p95 as f64 / 1_000_000.0),
        p99_ms = format!("{:.2}", final_stats.latency_ns_p99 as f64 / 1_000_000.0),
        duration = format!("{:.2}s", final_stats.total_duration.as_secs_f64()),
        "Final Subscriber Statistics"
    );

    // Write final snapshot to output
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }

    // Clean up
    if let Some(h) = snapshot_handle {
        h.abort();
    }
    // Graceful shutdown of subscription and transport
    let _ = subscription.shutdown().await;
    transport
        .shutdown()
        .await
        .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;

    Ok(())
}
