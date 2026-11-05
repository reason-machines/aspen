//! Tests for `aspen-planner`: hand-built invariants (`hand`) and property
//! tests against `git rev-list --objects` over random histories (`oracle`,
//! with the generator in `world`).

mod graph;
mod hand;
mod oracle;
mod world;
