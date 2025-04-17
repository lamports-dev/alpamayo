use {
    http_body_util::{BodyExt, combinators::BoxBody},
    hyper::{
        HeaderMap, StatusCode,
        body::{Body, Bytes},
        http::Result as HttpResult,
    },
    std::{fmt, sync::Arc},
};

pub const X_SUBSCRIPTION_ID: &str = "x-subscription-id";
pub const X_BIGTABLE: &str = "x-bigtable"; // https://github.com/anza-xyz/agave/blob/v2.2.10/rpc/src/rpc_service.rs#L554
pub const X_ERROR: &str = "x-error";
pub const X_SLOT: &str = "x-slot";

pub type RpcResponse = hyper::Response<BoxBody<Bytes, std::convert::Infallible>>;

pub fn check_call_support<T: Eq + fmt::Debug>(calls: &[T], call: T) -> anyhow::Result<bool> {
    let count = calls.iter().filter(|value| **value == call).count();
    anyhow::ensure!(count <= 1, "{call:?} defined multiple times");
    Ok(count == 1)
}

pub fn get_x_subscription_id(headers: &HeaderMap) -> Arc<str> {
    headers
        .get(X_SUBSCRIPTION_ID)
        .and_then(|value| value.to_str().ok().map(ToOwned::to_owned))
        .unwrap_or_default()
        .into()
}

pub fn get_x_bigtable_disabled(headers: &HeaderMap) -> bool {
    headers.get(X_BIGTABLE).is_some_and(|v| v == "disabled")
}

pub fn response_400<B>(body: B, x_error: Option<Vec<u8>>) -> HttpResult<RpcResponse>
where
    B: BodyExt + Send + Sync + 'static,
    B: Body<Data = Bytes, Error = std::convert::Infallible>,
{
    let mut response = hyper::Response::builder();
    if let Some(x_error) = x_error {
        response = response.header(X_ERROR, x_error);
    }
    response.status(StatusCode::BAD_REQUEST).body(body.boxed())
}

pub fn response_500<E: fmt::Display>(error: E) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(format!("{error}\n").boxed())
}
