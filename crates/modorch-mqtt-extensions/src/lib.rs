//! Optional source-authorized Admin and bounded aggregate Metrics Modules.
#![forbid(unsafe_code)]
#![deny(missing_docs)]
mod admin;
mod metrics;
pub use admin::*;
pub use metrics::*;

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
pub use http::{HttpConfig, HttpFactory, HttpHandle};

#[cfg(feature = "audit")]
mod audit;
#[cfg(feature = "audit")]
pub use audit::SqliteAuditConfig;
