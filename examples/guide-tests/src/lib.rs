//! Compiles the repository guides without adding external Markdown paths to
//! either published library package. Cargo's workspace doctest lane runs the
//! exact examples readers see, including ownership across successive steps.

#[doc = include_str!("../../../docs/quickstart.md")]
mod quickstart {}

#[doc = include_str!("../../../docs/reference/feature-flags.md")]
mod feature_flags {}
