use anyhow::Result;
use bytes::Bytes;
use futures::future::join_all;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use crate::metrics::stats::Stats;
use crate::payload::generate_payload;
use crate::rate::RateController;
use crate::transport::{ConnectOptions, Engine, Transport, TransportBuilder};

#[derive(Clone, Copy, Debug)]
pub enum KeyMappingMode {
    MDim,
    Hash,
}

/// Multi-topic fanout driver (single process)
/// Generates multi-segment keys like: {prefix}/t{tenant}/r{region}/svc{service}/k{shard}
/// and can drive many logical publishers without spawning many OS processes.
pub struct MultiTopicConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub topic_prefix: String,
    pub tenants: u32,
    pub regions: u32,
    pub services: u32,
    pub shards: u32,
    pub publishers: i64, // number of logical publishers (<= T*R*S*K); negative => use total_keys
    pub mapping: KeyMappingMode, // mapping mode from i -> (t,r,s,k)
    pub payload_size: usize,
    pub rate_per_pub: Option<f64>,
    pub duration_secs: u64,
    pub snapshot_interval_secs: u64,
    pub share_transport: bool, // when true, reuse one transport for all publishers
    pub ramp_up_secs: f64, // total ramp-up time in seconds (0 = no delay)
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, aggregate externally
    pub disable_internal_snapshot: bool,
}

fn fnv1a64(mut x: u64) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325; // offset basis
    let prime: u64 = 0x100000001b3;
    // mix 8 bytes of x
    for _ in 0..8 {
        let b = (x & 0xFF) as u8;
        h ^= b as u64;
        h = h.wrapping_mul(prime);
        x >>= 8;
    }
    h
}

fn map_index(i: u64, t: u32, r: u32, s: u32, k: u32, mode: KeyMappingMode) -> (u32, u32, u32, u32) {
    let t64 = t as u64;
    let r64 = r as u64;
    let s64 = s as u64;
    let k64 = k as u64;
    match mode {
        KeyMappingMode::MDim => {
            let ti = (i % t64) as u32;
            let ri = ((i / t64) % r64) as u32;
            let si = ((i / (t64 * r64)) % s64) as u32;
            let ki = ((i / (t64 * r64 * s64)) % k64) as u32;
            (ti, ri, si, ki)
        }
        KeyMappingMode::Hash => {
            let h = fnv1a64(i + 0x9e3779b97f4a7c15); // add golden ratio to spread low i
            let ti = (h % t64) as u32;
            let ri = ((h / t64) % r64) as u32;
            let si = ((h / (t64 * r64)) % s64) as u32;
            let ki = ((h / (t64 * r64 * s64)) % k64) as u32;
            (ti, ri, si, ki)
        }
    }
}

pub async fn run_multi_topic(config: MultiTopicConfig) -> Result<()> {
    // Determine total keys and effective publisher count
    let total_keys = (config.tenants as u64)
        .saturating_mul(config.regions as u64)
        .saturating_mul(config.services as u64)
        .saturating_mul(config.shards as u64);
    let pubs: u64 = if config.publishers < 0 {
        total_keys
    } else {
        (config.publishers as u64).min(total_keys)
    };

    println!(
        "[multi_topic] engine={:?} prefix={} dims=T{}xR{}xS{}xK{} pubs={} payload={} rate={:?} dur={}s",
        config.engine,
        config.topic_prefix,
        config.tenants,
        config.regions,
        config.services,
        config.shards,
        pubs,
        config.payload_size,
        config.rate_per_pub,
        config.duration_secs
    );

    // Shared vs per-key transport depending on config

    // Stats
    let stats: Arc<Stats> = config
        .shared_stats
        .clone()
        .unwrap_or_else(|| Arc::new(Stats::new()));

    // Optional internal snapshot
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = stats.clone();
        let every = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(every));
            loop {
                t.tick().await;
                let s = stats_clone.snapshot().await;
                println!(
                    "[multi_topic] sent={} err={} itps={:.2}",
                    s.sent_count,
                    s.error_count,
                    s.interval_throughput()
                );
            }
        }))
    } else {
        None
    };

    // Prepare publishers (pubs calculated above)

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(pubs as usize);

    if config.share_transport {
        println!("[multi_topic] using shared transport");
        let transport: Box<dyn Transport> =
            TransportBuilder::connect(config.engine.clone(), config.connect.clone())
                .await
                .map_err(|e| anyhow::Error::msg(format!("transport connect error: {}", e)))?;
        
        // Optimization: For high publisher counts (e.g. >1000), spawning one task per publisher
        // with its own RateController (timer) creates massive scheduling overhead.
        // Instead, we use a single task to drive all publishers in a round-robin fashion
        // if a rate is specified. If no rate (max speed), we still use per-pub tasks but without timers.
        
        let mut pub_handles = Vec::with_capacity(pubs as usize);
        for i in 0..pubs {
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let pub_handle = transport.create_publisher(&key).await.map_err(|e| {
                anyhow::Error::msg(format!("create_publisher error ({}): {}", key, e))
            })?;
            // Track connection created
            stats.increment_connections();
            pub_handles.push(pub_handle);
        }

        if let Some(rate) = config.rate_per_pub {
            // Sharded driver tasks for publishers to allow concurrency
            // Total target rate = rate * pubs
            let total_rate = rate * (pubs as f64);
            // Use a reasonable number of shards (e.g. 32) to allow concurrency without spawning per-pub tasks
            let num_shards = 32.min(pubs as usize).max(1);
            let rate_per_shard = total_rate / (num_shards as f64);
            
            println!("[multi_topic] optimizing: {} driver tasks for {} pubs @ total {:.2} msg/s ({:.2}/shard)", 
                num_shards, pubs, total_rate, rate_per_shard);
            
            // Distribute pub_handles into shards
            let mut pub_iter = pub_handles.into_iter();
            let chunk_size = (pubs as usize + num_shards - 1) / num_shards;

            for _ in 0..num_shards {
                let mut shard_pubs = Vec::with_capacity(chunk_size);
                for _ in 0..chunk_size {
                    if let Some(p) = pub_iter.next() {
                        shard_pubs.push(p);
                    }
                }
                if shard_pubs.is_empty() { break; }
                
                let stats_p = stats.clone();
                let payload_size = config.payload_size;
                let stop_flag = stop.clone();
                let shard_size = shard_pubs.len();
                
                handles.push(tokio::spawn(async move {
                    let mut rc = RateController::new(rate_per_shard);
                    let mut seq = 0u64;
                    let mut pub_idx = 0;
                    let num_pubs = shard_pubs.len();
                    let mut is_active = false;
                    
                    loop {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        rc.wait_for_next().await;
                        
                        let payload = generate_payload(seq, payload_size);
                        let bytes = Bytes::from(payload);
                        
                        // Round-robin publish within shard
                        if let Some(ph) = shard_pubs.get(pub_idx) {
                            match ph.publish(bytes).await {
                                Ok(_) => {
                                    if !is_active {
                                        // Activate all connections in this shard on first successful publish
                                        for _ in 0..shard_size {
                                            stats_p.increment_active_connections();
                                        }
                                        is_active = true;
                                    }
                                    stats_p.record_sent().await;
                                }
                                Err(e) => {
                                    if seq % 1000 == 0 {
                                        eprintln!("[multi_topic] send error (sample): {}", e);
                                    }
                                    stats_p.record_error().await;
                                }
                            }
                        }
                        
                        seq = seq.wrapping_add(1);
                        pub_idx = (pub_idx + 1) % num_pubs;
                    }
                    
                    // Track connection shutdown for all pubs in shard
                    if is_active {
                        for _ in 0..shard_size {
                            stats_p.decrement_active_connections();
                        }
                    }
                    for _ in 0..shard_size {
                        stats_p.decrement_connections();
                    }
                    
                    for ph in shard_pubs {
                        let _ = ph.shutdown().await;
                    }
                }));
            }
        } else {
            // No rate limit: spawn per-publisher tasks for max throughput
            for (_i, pub_handle) in pub_handles.into_iter().enumerate() {
                let stats_p = stats.clone();
                let payload_size = config.payload_size;
                let stop_flag = stop.clone();
                handles.push(tokio::spawn(async move {
                    let mut seq = 0u64;
                    let mut is_active = false;
                    loop {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        // No rate controller wait
                        let payload = generate_payload(seq, payload_size);
                        let bytes = Bytes::from(payload);
                        match pub_handle.publish(bytes).await {
                            Ok(_) => {
                                if !is_active {
                                    stats_p.increment_active_connections();
                                    is_active = true;
                                }
                                stats_p.record_sent().await;
                                seq = seq.wrapping_add(1);
                            }
                            Err(_e) => {
                                stats_p.record_error().await;
                            }
                        }
                    }
                    // Track connection shutdown
                    if is_active {
                        stats_p.decrement_active_connections();
                    }
                    stats_p.decrement_connections();
                    let _ = pub_handle.shutdown().await;
                }));
            }
        }

        // Wait and stop
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        stop.store(true, Ordering::Relaxed);
        let _ = join_all(handles).await;
        transport
            .shutdown()
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
        if let Some(h) = snapshot_handle {
            h.abort();
        }
        let final_stats = stats.snapshot().await;
        println!(
            "[multi_topic] done: sent={} errors={} total_tps={:.2}",
            final_stats.sent_count,
            final_stats.error_count,
            final_stats.total_throughput()
        );
        return Ok(());
    } else {
        // Calculate per-connection delay from total ramp-up time
        let ramp_delay_us = if config.ramp_up_secs > 0.0 && pubs > 1 {
            ((config.ramp_up_secs * 1_000_000.0) / (pubs - 1) as f64) as u64
        } else {
            0
        };
        println!("[multi_topic] using per-key transports (ramp_up={}s, delay_per_conn={}us)", 
            config.ramp_up_secs, ramp_delay_us);
        for i in 0..pubs {
            // Apply ramp-up delay between connections (skip first)
            if i > 0 && ramp_delay_us > 0 {
                tokio::time::sleep(Duration::from_micros(ramp_delay_us)).await;
            }
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let transport: Box<dyn Transport> =
                TransportBuilder::connect(config.engine.clone(), config.connect.clone())
                    .await
                    .map_err(|e| {
                        anyhow::Error::msg(format!("transport connect error ({}): {}", key, e))
                    })?;
            let pub_handle = transport.create_publisher(&key).await.map_err(|e| {
                anyhow::Error::msg(format!("create_publisher error ({}): {}", key, e))
            })?;
            // Track connection created
            stats.increment_connections();
            let stats_p = stats.clone();
            let rate = config.rate_per_pub;
            let payload_size = config.payload_size;
            let stop_flag = stop.clone();
            handles.push(tokio::spawn(async move {
                let mut rc = rate.map(RateController::new);
                let mut seq = 0u64;
                let mut is_active = false;
                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Some(r) = &mut rc {
                        r.wait_for_next().await;
                    }
                    let payload = generate_payload(seq, payload_size);
                    let bytes = Bytes::from(payload);
                    match pub_handle.publish(bytes).await {
                        Ok(_) => {
                            if !is_active {
                                stats_p.increment_active_connections();
                                is_active = true;
                            }
                            stats_p.record_sent().await;
                            seq = seq.wrapping_add(1);
                        }
                        Err(e) => {
                            eprintln!("[multi_topic] send error on {}: {}", key, e);
                            stats_p.record_error().await;
                        }
                    }
                }
                // Track connection shutdown
                if is_active {
                    stats_p.decrement_active_connections();
                }
                stats_p.decrement_connections();
                let _ = pub_handle.shutdown().await;
                let _ = transport.shutdown().await;
            }));
        }
    }

    // Wait for either duration or Ctrl+C
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
        _ = tokio::signal::ctrl_c() => {},
    }
    stop.store(true, Ordering::Relaxed);

    // Wait for all publisher tasks to exit gracefully
    let _ = join_all(handles).await;

    if let Some(h) = snapshot_handle {
        h.abort();
    }

    // Final stats
    let final_stats = stats.snapshot().await;
    println!(
        "[multi_topic] done: sent={} errors={} total_tps={:.2}",
        final_stats.sent_count,
        final_stats.error_count,
        final_stats.total_throughput()
    );

    Ok(())
}

// ========================= MULTI-TOPIC SUBSCRIBER =========================

pub struct MultiTopicSubConfig {
    pub engine: Engine,
    pub connect: ConnectOptions,
    pub topic_prefix: String,
    pub tenants: u32,
    pub regions: u32,
    pub services: u32,
    pub shards: u32,
    pub subscribers: i64, // number of per-key subscriptions (<= T*R*S*K); negative => use total_keys
    pub mapping: KeyMappingMode,
    pub duration_secs: u64,
    pub snapshot_interval_secs: u64,
    pub share_transport: bool, // when true, reuse one transport for all subscriptions
    pub ramp_up_secs: f64, // total ramp-up time in seconds (0 = no delay)
    // Aggregation support
    pub shared_stats: Option<Arc<Stats>>, // when set, aggregate externally
    pub disable_internal_snapshot: bool,
}

use crate::payload::parse_header;
use crate::time_sync::now_unix_ns_estimate;

pub async fn run_multi_topic_sub(config: MultiTopicSubConfig) -> Result<()> {
    // Compute total keys and effective subscriber count
    let total_keys = (config.tenants as u64)
        .saturating_mul(config.regions as u64)
        .saturating_mul(config.services as u64)
        .saturating_mul(config.shards as u64);
    let subs: u64 = if config.subscribers < 0 {
        total_keys
    } else {
        (config.subscribers as u64).min(total_keys)
    };
    println!(
        "[multi_topic_sub] engine={:?} prefix={} dims=T{}xR{}xS{}xK{} subs={} dur={}s",
        config.engine,
        config.topic_prefix,
        config.tenants,
        config.regions,
        config.services,
        config.shards,
        subs,
        config.duration_secs
    );

    // Note: shared vs per-subscription transport

    // Stats (use shared aggregator if provided)
    let stats: Arc<Stats> = config
        .shared_stats
        .clone()
        .unwrap_or_else(|| Arc::new(Stats::new()));

    // Optional internal snapshot to stdout when not aggregated
    let snapshot_handle = if !config.disable_internal_snapshot {
        let stats_clone = stats.clone();
        let every = config.snapshot_interval_secs;
        Some(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(every));
            loop {
                t.tick().await;
                let s = stats_clone.snapshot().await;
                println!(
                    "[multi_topic_sub] recv={} err={} itps={:.2} p99={:.2}ms",
                    s.received_count,
                    s.error_count,
                    s.interval_throughput(),
                    s.latency_ns_p99 as f64 / 1_000_000.0
                );
            }
        }))
    } else {
        None
    };

    // Batched stats worker via channel
    // Increase channel capacity to avoid backpressure on high-throughput scenarios
    let (tx, rx) = flume::bounded::<(u64, [u8; 24])>(1_000_000);
    let stats_worker = stats.clone();
    
    // Spawn multiple stats workers to parallelize histogram recording if needed
    // But histogram is protected by RwLock, so single writer is better.
    // However, we can optimize the batch size and loop.
    tokio::spawn(async move {
        let mut buf = Vec::with_capacity(4096);
        let mut lats = Vec::with_capacity(4096);
        loop {
            let first = match rx.recv_async().await {
                Ok(v) => v,
                Err(_) => break,
            };
            buf.clear();
            buf.push(first);
            // Drain more aggressively
            while let Ok(v) = rx.try_recv() {
                buf.push(v);
                if buf.len() >= 4096 {
                    break;
                }
            }
            lats.clear();
            for (recv_ns, hdr) in buf.drain(..) {
                if let Ok(h) = parse_header(&hdr) {
                    lats.push(recv_ns.saturating_sub(h.timestamp_ns));
                }
            }
            stats_worker.record_received_batch(&lats).await;
        }
    });

    // Create per-key subscriptions (subs computed above)

    if config.share_transport {
        println!("[multi_topic_sub] using shared transport");
        let transport: Box<dyn Transport> =
            TransportBuilder::connect(config.engine.clone(), config.connect.clone())
                .await
                .map_err(|e| anyhow::Error::msg(format!("transport connect error: {}", e)))?;
        let mut subs_vec: Vec<(Box<dyn crate::transport::Subscription>, Arc<AtomicBool>)> =
            Vec::with_capacity(subs as usize);
        for i in 0..subs {
            let (t, r, s, k) = map_index(
                i,
                config.tenants,
                config.regions,
                config.services,
                config.shards,
                config.mapping,
            );
            let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
            let handler_tx = tx.clone();
            let stats_cb = stats.clone();
            let first_received = Arc::new(AtomicBool::new(false));
            let first_received_cb = first_received.clone();
            let sub = transport
                .subscribe(
                    &key,
                    Box::new(move |msg: crate::transport::TransportMessage| {
                        let mut hdr = [0u8; 24];
                        let bytes = msg.payload.as_cow();
                        if bytes.len() >= 24 {
                            hdr.copy_from_slice(&bytes[..24]);
                            let recv = now_unix_ns_estimate();
                            let _ = handler_tx.try_send((recv, hdr));
                            // Track first receive for active connection
                            if !first_received_cb.swap(true, Ordering::Relaxed) {
                                stats_cb.increment_active_connections();
                            }
                        }
                    }),
                )
                .await
                .map_err(|e| anyhow::Error::msg(format!("subscribe error on {}: {}", key, e)))?;
            // Track connection created
            stats.increment_connections();
            subs_vec.push((sub, first_received));
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
            _ = tokio::signal::ctrl_c() => {},
        }
        for (s, first_received) in &subs_vec {
            let _ = s.shutdown().await;
            // Track connection shutdown
            if first_received.load(Ordering::Relaxed) {
                stats.decrement_active_connections();
            }
            stats.decrement_connections();
        }
        transport
            .shutdown()
            .await
            .map_err(|e| anyhow::Error::msg(format!("transport shutdown error: {}", e)))?;
        if let Some(h) = snapshot_handle {
            h.abort();
        }
        let s = stats.snapshot().await;
        println!(
            "[multi_topic_sub] done: recv={} errors={} total_tps={:.2} p99={:.2}ms",
            s.received_count,
            s.error_count,
            s.total_throughput(),
            s.latency_ns_p99 as f64 / 1_000_000.0
        );
        return Ok(());
    }

    // Per-subscription transports
    // Calculate per-connection delay from total ramp-up time
    let ramp_delay_us = if config.ramp_up_secs > 0.0 && subs > 1 {
        ((config.ramp_up_secs * 1_000_000.0) / (subs - 1) as f64) as u64
    } else {
        0
    };
    println!("[multi_topic_sub] using per-key transports (ramp_up={}s, delay_per_conn={}us)", 
        config.ramp_up_secs, ramp_delay_us);
    // Hold both the subscription and its own transport to keep the client alive
    let mut clients: Vec<(Box<dyn crate::transport::Subscription>, Box<dyn Transport>, Arc<AtomicBool>)> =
        Vec::with_capacity(subs as usize);
    for i in 0..subs {
        // Apply ramp-up delay between connections (skip first)
        if i > 0 && ramp_delay_us > 0 {
            tokio::time::sleep(Duration::from_micros(ramp_delay_us)).await;
        }
        let (t, r, s, k) = map_index(
            i,
            config.tenants,
            config.regions,
            config.services,
            config.shards,
            config.mapping,
        );
        let key = format!("{}/t{}/r{}/svc{}/k{}", config.topic_prefix, t, r, s, k);
        let handler_tx = tx.clone();
        let stats_cb = stats.clone();
        let first_received = Arc::new(AtomicBool::new(false));
        let first_received_cb = first_received.clone();
        let transport: Box<dyn Transport> =
            TransportBuilder::connect(config.engine.clone(), config.connect.clone())
                .await
                .map_err(|e| {
                    anyhow::Error::msg(format!("transport connect error ({}): {}", key, e))
                })?;
        let sub = transport
            .subscribe(
                &key,
                Box::new(move |msg: crate::transport::TransportMessage| {
                    let mut hdr = [0u8; 24];
                    let bytes = msg.payload.as_cow();
                    if bytes.len() >= 24 {
                        hdr.copy_from_slice(&bytes[..24]);
                        let recv = now_unix_ns_estimate();
                        let _ = handler_tx.try_send((recv, hdr));
                        // Track first receive for active connection
                        if !first_received_cb.swap(true, Ordering::Relaxed) {
                            stats_cb.increment_active_connections();
                        }
                    }
                }),
            )
            .await
            .map_err(|e| anyhow::Error::msg(format!("subscribe error on {}: {}", key, e)))?;
        // Track connection created
        stats.increment_connections();
        clients.push((sub, transport, first_received));
    }

    // Wait for either duration or Ctrl+C
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(config.duration_secs)) => {},
        _ = tokio::signal::ctrl_c() => {},
    }

    // Clean up subscriptions and transports
    for (sub, trans, first_received) in &clients {
        let _ = sub.shutdown().await;
        let _ = trans.shutdown().await;
        // Track connection shutdown
        if first_received.load(Ordering::Relaxed) {
            stats.decrement_active_connections();
        }
        stats.decrement_connections();
    }
    if let Some(h) = snapshot_handle {
        h.abort();
    }

    // Final stats
    let s = stats.snapshot().await;
    println!(
        "[multi_topic_sub] done: recv={} errors={} total_tps={:.2} p99={:.2}ms",
        s.received_count,
        s.error_count,
        s.total_throughput(),
        s.latency_ns_p99 as f64 / 1_000_000.0
    );

    Ok(())
}
