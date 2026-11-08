//! The ledger: the nodes that hold the replicated logs. Every repository
//! has its own log ([`consensus`]), and one root log says which
//! repositories exist and who the members are ([`consensus::root`]); the
//! ledger creates and deletes repositories through it ([`Ledger`]).

#![deny(unsafe_code)]

pub mod cluster;
pub mod consensus;
mod ledger;
mod logdb;
mod membership;
mod proto;
mod state;
mod token;
mod watch;

pub use ledger::{Ledger, LedgerConfig};
pub use token::{Claims, Refused};
