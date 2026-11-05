//! Logs and traces for every `aspen` command (node C5).
//!
//! - **Logs** go to stderr through `tracing-subscriber`: human-readable
//!   text by default, one JSON object per line with `ASPEN_LOG_FORMAT=json`.
//!   `ASPEN_LOG` picks what is logged, in `tracing`'s filter syntax
//!   (default `info`; for example `aspen_server=debug,info`).
//! - **Traces** are exported over OTLP/HTTP (protobuf) when an OTLP
//!   endpoint is configured with the standard OpenTelemetry variables:
//!   `OTEL_EXPORTER_OTLP_ENDPOINT` (the exporter appends `/v1/traces`) or
//!   `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (used as is), plus the optional
//!   `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`,
//!   `OTEL_SERVICE_NAME` (default `aspen`) and `OTEL_RESOURCE_ATTRIBUTES`.
//!   `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` turns export
//!   off. Spans are batched on a background thread and flushed when the
//!   command ends ([`Telemetry`]'s `Drop`).
//!
//! Only `http/protobuf` is supported: an `OTEL_EXPORTER_OTLP_PROTOCOL` (or
//! `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`) naming anything else is refused at
//! startup rather than ignored. gRPC would need `tonic`, which nothing else
//! in Aspen uses, and every OpenTelemetry Collector accepts OTLP/HTTP on
//! port 4318.
//!
//! Metrics are not exported over OTLP: the server exposes them for
//! Prometheus at `/-/metrics` (`docs/OBSERVABILITY.md`), and an
//! OpenTelemetry Collector can scrape that with its `prometheus` receiver.

use std::io::IsTerminal;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

/// Keeps the trace exporter alive; dropping it flushes and shuts it down,
/// so every span of the command reaches the collector before the process
/// exits. Hold it in `main` until the command returns.
#[derive(Debug)]
pub(crate) struct Telemetry {
    provider: Option<SdkTracerProvider>,
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("aspen: flushing traces failed: {e}");
        }
    }
}

/// How logs are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

/// Telemetry settings read from the environment.
#[derive(Debug, PartialEq, Eq)]
struct Settings {
    filter: String,
    format: LogFormat,
    /// Export traces over OTLP/HTTP.
    otlp: bool,
    /// `service.name` to set, when `OTEL_SERVICE_NAME` does not.
    service_name: Option<&'static str>,
}

impl Settings {
    /// Reads the settings through `get` (the process environment in
    /// [`init`]; a map in tests). An empty variable counts as unset.
    fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let get = |k: &str| get(k).filter(|v| !v.trim().is_empty());
        let format = match get("ASPEN_LOG_FORMAT").as_deref() {
            None | Some("text") => LogFormat::Text,
            Some("json") => LogFormat::Json,
            Some(other) => {
                return Err(format!(
                    "ASPEN_LOG_FORMAT must be `text` or `json`, not {other:?}"
                ));
            }
        };
        for var in [
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
            "OTEL_EXPORTER_OTLP_PROTOCOL",
        ] {
            if let Some(p) = get(var)
                && p != "http/protobuf"
            {
                return Err(format!(
                    "{var}={p:?} is not supported; Aspen exports traces over OTLP/HTTP only \
                     (http/protobuf, usually port 4318)"
                ));
            }
        }
        let disabled = get("OTEL_SDK_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true"))
            || get("OTEL_TRACES_EXPORTER").is_some_and(|v| v == "none");
        let endpoint = get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .or_else(|| get("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .is_some();
        Ok(Self {
            filter: get("ASPEN_LOG").unwrap_or_else(|| "info".to_owned()),
            format,
            otlp: endpoint && !disabled,
            service_name: get("OTEL_SERVICE_NAME").is_none().then_some("aspen"),
        })
    }
}

/// Installs the global log subscriber and, when configured, the OTLP trace
/// exporter. Call once, first thing in `main`.
pub(crate) fn init() -> Result<Telemetry, String> {
    let settings = Settings::from_env(|k| std::env::var(k).ok())?;
    let filter = EnvFilter::try_new(&settings.filter)
        .map_err(|e| format!("ASPEN_LOG={:?}: {e}", settings.filter))?;
    let provider = settings.otlp.then(tracer_provider).transpose()?;
    let ansi = std::io::stderr().is_terminal();
    let text = (settings.format == LogFormat::Text).then(|| {
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_ansi(ansi)
    });
    let json = (settings.format == LogFormat::Json).then(|| {
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .with_writer(std::io::stderr)
    });
    let otel = provider
        .as_ref()
        .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("aspen")));
    tracing_subscriber::registry()
        .with(filter)
        .with(text)
        .with(json)
        .with(otel)
        .try_init()
        .map_err(|e| format!("installing the log subscriber: {e}"))?;
    Ok(Telemetry { provider })
}

fn tracer_provider() -> Result<SdkTracerProvider, String> {
    // An `https://` collector needs a rustls crypto provider; the blob
    // store's S3 client installs the same one (A4).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .map_err(|e| format!("OTLP trace exporter: {e}"))?;
    let mut resource = Resource::builder().with_attribute(opentelemetry::KeyValue::new(
        "service.version",
        aspen_core::VERSION,
    ));
    if let Some(name) = Settings::from_env(|k| std::env::var(k).ok())
        .ok()
        .and_then(|s| s.service_name)
    {
        resource = resource.with_service_name(name);
    }
    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.build())
        .build())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Result<Settings, String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        Settings::from_env(|k| map.get(k).cloned())
    }

    /// Without an OTLP endpoint nothing is exported, and logs are text at
    /// `info`.
    #[test]
    fn defaults_log_text_and_export_nothing() {
        assert_eq!(
            settings(&[]).unwrap(),
            Settings {
                filter: "info".into(),
                format: LogFormat::Text,
                otlp: false,
                service_name: Some("aspen"),
            }
        );
    }

    /// Either endpoint variable turns export on; the SDK's own off
    /// switches turn it off again.
    #[test]
    fn an_endpoint_enables_export_unless_disabled() {
        for var in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        ] {
            assert!(settings(&[(var, "http://c:4318")]).unwrap().otlp, "{var}");
            assert!(
                !settings(&[(var, "http://c:4318"), ("OTEL_SDK_DISABLED", "TRUE")])
                    .unwrap()
                    .otlp
            );
            assert!(
                !settings(&[(var, "http://c:4318"), ("OTEL_TRACES_EXPORTER", "none")])
                    .unwrap()
                    .otlp
            );
        }
        assert!(
            !settings(&[("OTEL_EXPORTER_OTLP_ENDPOINT", " ")])
                .unwrap()
                .otlp,
            "empty is unset"
        );
    }

    /// A protocol Aspen does not speak, or an unknown log format, is an
    /// error at startup, never silently ignored.
    #[test]
    fn unsupported_settings_are_refused() {
        let e = settings(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")]).unwrap_err();
        assert!(e.contains("http/protobuf"), "{e}");
        assert!(settings(&[("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json")]).is_err());
        assert!(settings(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")]).is_ok());
        assert!(settings(&[("ASPEN_LOG_FORMAT", "yaml")]).is_err());
        assert_eq!(
            settings(&[("ASPEN_LOG_FORMAT", "json"), ("ASPEN_LOG", "debug")]).unwrap(),
            Settings {
                filter: "debug".into(),
                format: LogFormat::Json,
                otlp: false,
                service_name: Some("aspen"),
            }
        );
        assert_eq!(
            settings(&[("OTEL_SERVICE_NAME", "aspen-eu")])
                .unwrap()
                .service_name,
            None,
            "OTEL_SERVICE_NAME wins"
        );
    }
}
