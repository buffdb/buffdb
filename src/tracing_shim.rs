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
pub(crate) use tracing::{debug, error, event, info, trace, warn, Level};

#[cfg(not(feature = "tracing"))]
pub(crate) struct Level(LevelInner);

#[cfg(not(feature = "tracing"))]
struct LevelInner;

#[cfg(not(feature = "tracing"))]
impl Level {
    pub(crate) const TRACE: Level = Level(LevelInner);
    pub(crate) const DEBUG: Level = Level(LevelInner);
    pub(crate) const INFO: Level = Level(LevelInner);
    pub(crate) const WARN: Level = Level(LevelInner);
    pub(crate) const ERROR: Level = Level(LevelInner);
}

#[cfg(not(feature = "tracing"))]
macro_rules! event {
    ($($x:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
pub(crate) use {
    event, event as debug, event as error, event as info, event as trace, event as warn,
};
