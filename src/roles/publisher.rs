use crate::metrics::stats::Stats;
use crate::output::OutputWriter;
use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::transport::{ConnectOptions, Engine, Transport, TransportBuilder};
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

pub struct PublisherConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub key_expr: String,
    pub payload_size: usize,
    pub rate: Option<f64>,
    pub duration_secs: Option<u64>,
    pub output_file: Option<String>,
    pub snapshot_interval_secs: u64,
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, use this shared collector
    pub disable_internal_snapshot: bool,  // when true, do not launch internal snapshot logger
}

pub async fn run_publisher(config: PublisherConfig) -> Result<()> {
    info!(
        engine = ?config.engine,
        key = %config.key_expr,
        payload_size = config.payload_size,
        rate = ?config.rate,
        duration_secs = ?config.duration_secs,
        endpoint = ?config.connect.params.get("endpoint"),
        "Starting publisher"
    );

    // Initialize statistics early so we can track connection failures
    let stats = if let Some(s) = &config.shared_stats {
        s.clone()
    } else {
        Arc::new(Stats::new())
    };

    // Initialize Transport with optional retry (generic engine + connect options)
    stats.record_connection_attempt();
    let transport: Box<dyn Transport> = match TransportBuilder::connect_with_retry(
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

    // Pre-declare publisher for performance
    let publisher = match transport.create_publisher(&config.key_expr).await {
        Ok(p) => p,
        Err(e) => {
            error!(error = %e, "Create publisher error");
            stats.record_connection_failure();
            return Ok(());
        }
    };

    let mut rate_controller = config.rate.map(|r| RateController::new(r));

    // Setup output writer (only when not aggregated)
    let mut output = if let Some(ref path) = config.output_file {
        Some(OutputWriter::new_csv(path.clone()).await?)
    } else if config.shared_stats.is_none() {
        Some(OutputWriter::new_stdout())
    } else {
        None
    };

    // Start snapshot task
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = Arc::clone(&stats);
        let interval_secs = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(interval_secs));
            loop {
                interval_timer.tick().await;
                let snapshot = stats_clone.snapshot().await;
                // Compute avg and interval send rates
                let elapsed = snapshot.total_duration.as_secs_f64();
                let avg_send_rate = if elapsed > 0.0 {
                    snapshot.sent_count as f64 / elapsed
                } else {
                    0.0
                };
                let inst_send_rate = if snapshot.interval_duration.as_secs_f64() > 0.0 {
                    snapshot.interval_sent_count as f64 / snapshot.interval_duration.as_secs_f64()
                } else {
                    0.0
                };
                debug!(
                    sent = snapshot.sent_count,
                    errors = snapshot.error_count,
                    avg_rate = format!("{:.2}", avg_send_rate),
                    inst_rate = format!("{:.2}", inst_send_rate),
                    "Publisher stats"
                );
            }
        }))
    } else {
        None
    };

    // Publishing loop
    let mut sequence = 0u64;
    let start_time = std::time::Instant::now();

    let publishing_task = async {
        loop {
            // Check duration limit
            if let Some(duration) = config.duration_secs {
                if start_time.elapsed().as_secs() >= duration {
                    info!("Duration limit reached, stopping publisher");
                    break;
                }
            }

            // Wait for next scheduled send (if paced)
            if let Some(rc) = &mut rate_controller {
                rc.wait_for_next().await;
            }

            // Generate and send payload
            let payload = generate_payload(sequence, config.payload_size);
            let bytes = Bytes::from(payload);

            match publisher.publish(bytes).await {
                Ok(_) => {
                    stats.record_sent().await;
                    sequence += 1;
                }
                Err(e) => {
                    warn!(error = %e, "Send error");
                    stats.record_error().await;
                }
            }
        }
    };

    // Wait for either completion or Ctrl+C
    tokio::select! {
        _ = publishing_task => {
            info!("Publishing completed");
        }
        _ = signal::ctrl_c() => {
            info!("Ctrl+C received, stopping publisher");
        }
    }

    // Final statistics
    let final_stats = stats.snapshot().await;
    let total_elapsed = final_stats.total_duration.as_secs_f64();
    let avg_send_rate = if total_elapsed > 0.0 {
        final_stats.sent_count as f64 / total_elapsed
    } else {
        0.0
    };
    info!(
        sent = final_stats.sent_count,
        errors = final_stats.error_count,
        avg_rate = format!("{:.2}", avg_send_rate),
        duration = format!("{:.2}s", total_elapsed),
        "Final Publisher Statistics"
    );

    // Write final snapshot to output (if not aggregated)
    if let Some(ref mut out) = output {
        out.write_snapshot(&final_stats).await?;
    }

    // Clean up
    if let Some(h) = snapshot_handle {
        h.abort();
    }
    transport
        .shutdown()
        .await
        .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;

    Ok(())
}
