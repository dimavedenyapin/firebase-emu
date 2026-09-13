//! HTTP boundaries shared by the loopback emulator listeners.

use axum::{
    extract::Request,
    http::{header, uri::Authority, HeaderMap, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
};

pub(crate) fn has_loopback_authority(headers: &HeaderMap, uri: &Uri) -> bool {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| uri.authority().map(Authority::as_str))
        .and_then(|value| value.parse::<Authority>().ok())
        .is_some_and(|authority| {
            let host = authority
                .host()
                .trim_start_matches('[')
                .trim_end_matches(']');
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        })
}

fn is_loopback_origin(value: &str) -> bool {
    let Ok(uri) = value.parse::<Uri>() else {
        return false;
    };
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.path() != "/"
        || uri.query().is_some()
    {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn has_safe_browser_origin(headers: &HeaderMap) -> bool {
    headers
        .get(header::ORIGIN)
        .is_none_or(|value| value.to_str().is_ok_and(is_loopback_origin))
}

fn forbidden() -> Response {
    let mut response = (
        StatusCode::FORBIDDEN,
        "emulator requests require a loopback Host and browser Origin",
    )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        "nosniff".parse().expect("static header"),
    );
    response
}

pub(crate) async fn require_loopback_request(request: Request, next: Next) -> Response {
    if !has_loopback_authority(request.headers(), request.uri())
        || !has_safe_browser_origin(request.headers())
    {
        return forbidden();
    }
    next.run(request).await
}

pub(crate) async fn require_loopback_request_axum07(
    request: axum07::extract::Request,
    next: axum07::middleware::Next,
) -> axum07::response::Response {
    if !has_loopback_authority(request.headers(), request.uri())
        || !has_safe_browser_origin(request.headers())
    {
        let mut response = axum07::response::IntoResponse::into_response((
            axum07::http::StatusCode::FORBIDDEN,
            "emulator requests require a loopback Host and browser Origin",
        ));
        response.headers_mut().insert(
            axum07::http::header::CACHE_CONTROL,
            "no-store".parse().expect("static header"),
        );
        response.headers_mut().insert(
            axum07::http::header::X_CONTENT_TYPE_OPTIONS,
            "nosniff".parse().expect("static header"),
        );
        return response;
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_loopback_hosts_and_http2_authority() {
        for value in [
            "127.0.0.1:8080",
            "127.9.8.7:42",
            "[::1]:9099",
            "LOCALHOST:5001",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, value.parse().unwrap());
            assert!(has_loopback_authority(&headers, &Uri::from_static("/")));
        }
        assert!(has_loopback_authority(
            &HeaderMap::new(),
            &"http://[::1]:8080/v1/test".parse().unwrap()
        ));
    }

    #[test]
    fn rejects_missing_external_and_malformed_hosts() {
        assert!(!has_loopback_authority(
            &HeaderMap::new(),
            &Uri::from_static("/")
        ));
        for value in [
            "attacker.example:8080",
            "0.0.0.0:8080",
            "[::]:8080",
            "127.0.0.1.example",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, value.parse().unwrap());
            assert!(!has_loopback_authority(&headers, &Uri::from_static("/")));
        }
    }

    #[test]
    fn accepts_only_loopback_browser_origins() {
        for value in [
            "http://127.0.0.1:3000",
            "https://127.1.2.3:8443",
            "http://[::1]:5173",
            "http://LOCALHOST:4173",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ORIGIN, value.parse().unwrap());
            assert!(has_safe_browser_origin(&headers));
        }
        assert!(has_safe_browser_origin(&HeaderMap::new()));
        for value in [
            "null",
            "https://attacker.example",
            "http://0.0.0.0:3000",
            "file://localhost/tmp/app.html",
            "http://localhost:3000/not-an-origin",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ORIGIN, value.parse().unwrap());
            assert!(!has_safe_browser_origin(&headers));
        }
    }
}
