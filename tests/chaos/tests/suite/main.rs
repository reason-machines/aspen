//! The chaos suite's required-CI tests, in one binary so they run in
//! parallel: the checks against servers with known behaviour, the guards
//! that fail a run which checked nothing, and the `smoke` scenario against
//! real `aspen serve` processes.

mod aspen;
mod checks_catch_bugs;
mod guards;
