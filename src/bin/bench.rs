use {
    anyhow::Context,
    clap::Parser,
    futures::future::try_join_all,
    hyper::{StatusCode, header::CONTENT_TYPE},
    quanta::Instant,
    rand::random_range,
    reqwest::Client,
    solana_rpc_client_api::config::RpcBlockConfig,
    solana_sdk::clock::Slot,
    std::{ops::Range, sync::Arc, time::Duration},
    tokio::sync::Mutex,
    url::Url,
};

#[derive(Debug, Parser)]
struct Args {
    /// Alpamayo endpoint
    #[clap(long, default_value_t = String::from("http://127.0.0.1:9000"))]
    endpoint: String,

    /// Slots interval, like m..n
    #[clap(long)]
    interval: String,

    /// How many blocks to request
    #[clap(long, default_value_t = 5)]
    count: usize,

    /// Requests concurrency
    #[clap(long, default_value_t = 1)]
    concurrency: usize,

    /// Request only http/get
    #[clap(long, default_value_t = false)]
    only_httpget: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let url = Url::parse(&args.endpoint)?;

    let Some((start, end)) = args.interval.split_once("..") else {
        anyhow::bail!("expected valid interval");
    };
    let range = Range {
        start: start.parse::<Slot>().context("failed to parse start")?,
        end: end.parse().context("failed to parse end")?,
    };

    let count = Arc::new(Mutex::new(args.count));

    let elapsed_jsonrpc = Arc::new(Mutex::new(Duration::ZERO));
    let elapsed_httpget = Arc::new(Mutex::new(Duration::ZERO));

    try_join_all((0..args.concurrency).map(|_| {
        make_requests(
            url.clone(),
            range.clone(),
            Arc::clone(&count),
            args.only_httpget,
            Arc::clone(&elapsed_jsonrpc),
            Arc::clone(&elapsed_httpget),
        )
    }))
    .await?;

    let elapsed_jsonrpc = *elapsed_jsonrpc.lock().await;
    println!(
        "jsonrpc: total {elapsed_jsonrpc:?} / avg: {:?}",
        elapsed_jsonrpc.div_f64(args.count as f64)
    );
    let elapsed_httpget = *elapsed_httpget.lock().await;
    println!(
        "httpget: total {elapsed_httpget:?} / avg: {:?}",
        elapsed_httpget.div_f64(args.count as f64)
    );

    Ok(())
}

async fn make_requests(
    url: Url,
    range: Range<Slot>,
    count: Arc<Mutex<usize>>,
    only_httpget: bool,
    elapsed_jsonrpc: Arc<Mutex<Duration>>,
    elapsed_httpget: Arc<Mutex<Duration>>,
) -> anyhow::Result<()> {
    let mut skip = false;
    loop {
        if skip {
            skip = false;
        } else {
            let mut locked = count.lock().await;
            if *locked == 0 {
                break;
            }
            *locked -= 1;
            drop(locked);
        }

        let slot = random_range(range.clone());

        // warmup
        if !fetch_slot_get(url.clone(), slot).await? {
            skip = true;
            continue;
        }
        if !only_httpget {
            fetch_slot_json(url.clone(), slot).await?;
        }

        // measure
        let ts = Instant::now();
        let _ = fetch_slot_get(url.clone(), slot).await?;
        *elapsed_httpget.lock().await += ts.elapsed();
        if !only_httpget {
            let ts = Instant::now();
            fetch_slot_json(url.clone(), slot).await?;
            *elapsed_jsonrpc.lock().await += ts.elapsed();
        }
    }

    Ok(())
}

async fn fetch_slot_json(url: Url, slot: Slot) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "getBlock",
        "id": 0,
        "params": [slot, RpcBlockConfig {
            max_supported_transaction_version: Some(0),
            ..Default::default()
        }]
    })
    .to_string();

    let response = Client::builder()
        .build()
        .context("failed to build http client")?
        .post(url.to_string())
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .context("failed to send jsonrpc request")?;

    anyhow::ensure!(
        response.status() == StatusCode::OK,
        "unexpected response code from jsonrpc {}",
        response.status(),
    );

    let _bytes = response
        .bytes()
        .await
        .context("failed to fetch body of jsonrpc request")?;

    Ok(())
}

async fn fetch_slot_get(mut url: Url, slot: Slot) -> anyhow::Result<bool> {
    let slot = slot.to_string();
    if let Ok(mut segments) = url.path_segments_mut() {
        segments.extend(&["block", &slot]);
    }

    let response = Client::builder()
        .build()
        .context("failed to build http client")?
        .get(url.to_string())
        .send()
        .await
        .context("failed to send jsonrpc request")?;

    if response.status() == StatusCode::BAD_REQUEST {
        return Ok(false);
    }

    anyhow::ensure!(
        response.status() == StatusCode::OK,
        "unexpected response code from httpget {}",
        response.status(),
    );

    let _bytes = response
        .bytes()
        .await
        .context("failed to fetch body of jsonrpc request")?;

    Ok(true)
}
