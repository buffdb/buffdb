//! Either `tracing` or equivalent stubs that can be used in its place.
//!
//! This is intended to be used instead of importing macros, types, etc. from `tracing` directly so
//! as to avoid placing all `tracing`-related items behind a `#[cfg]`. The only exception is the
//! `#[instrument]` attribute, which needs to be used as
//! `#[cfg_attr(feature = "tracing", tracing::instrument)]`.
//!
//! Note that this module is not exhaustive. The API can be expanded as needed.

#![allow(unused_imports, unused_macros, dead_code)]

#[cfg(feature = "tracing")]
pub(crate) use tracing::{
    debug, debug_span, error, error_span, event, info, info_span, trace, trace_span, warn,
    warn_span, Level,
};
#[cfg(feature = "tracing")]
pub(crate) use tracing_futures::Instrument;

#[cfg(not(feature = "tracing"))]
pub(crate) struct Level(LevelInner);

#[cfg(not(feature = "tracing"))]
struct LevelInner;

#[cfg(not(feature = "tracing"))]
impl Level {
    pub(crate) const TRACE: Self = Self(LevelInner);
    pub(crate) const DEBUG: Self = Self(LevelInner);
    pub(crate) const INFO: Self = Self(LevelInner);
    pub(crate) const WARN: Self = Self(LevelInner);
    pub(crate) const ERROR: Self = Self(LevelInner);
}

#[cfg(not(feature = "tracing"))]
macro_rules! event {
    ($($x:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
macro_rules! event_span {
    ($($x:tt)*) => {
        ()
    };
}

#[cfg(not(feature = "tracing"))]
pub(crate) use {
    event, event as debug, event as error, event as info, event as trace, event as warn,
    event_span, event_span as debug_span, event_span as error_span, event_span as info_span,
    event_span as trace_span, event_span as warn_span,
};

#[cfg(not(feature = "tracing"))]
pub(crate) trait Instrument {
    fn instrument(self, span: ()) -> Self;
}

#[cfg(not(feature = "tracing"))]
impl<T> Instrument for T {
    fn instrument(self, _: ()) -> Self {
        self
    }
}
