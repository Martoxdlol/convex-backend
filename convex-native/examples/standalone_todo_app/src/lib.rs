//! Library root — declares every module that contains
//! `#[convex::*]` or `#[derive(ConvexDocument)]`. Without these
//! `pub mod` lines the linker drops the `inventory::submit!`
//! entries emitted by the macros and the `NativeFunctionRunner`
//! sees an empty registry at startup.
pub mod app {
    pub mod actions;
    pub mod mutations;
    pub mod queries;
    pub mod schema;
}
