//! One write's index rows, held until they all land together.
//!
//! [`Journal`] is the write as a value — what was listed, what pack images it
//! keeps alive, and which it retires. [`Ledger`] lands one, all of it or none.
//!
//! # Why one value
//! The engine keeps four segmented lists, and a commit listed in the graph
//! while its objects are not is one no later push ever records the objects of.
//!
//! # Why a value first
//! Building a journal does no I/O, so a caller encodes its segments and puts
//! the large ones in the bucket before anything opens a transaction.

mod journal;
mod ledger;
mod memory;

pub use journal::{Index, Journal, Listing};
pub use ledger::Ledger;
pub use memory::MemoryLedger;
