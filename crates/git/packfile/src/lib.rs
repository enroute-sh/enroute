//! Reading git packfiles: entry framing, streaming decode, and delta
//! application.
//!
//! Sits below both the wire protocol and the push path because it is neither:
//! a packfile is a storage format that arrives over a transport, so the code
//! that decodes one belongs to neither the transport nor the ingest pipeline
//! that consumes the result.

mod delta;
mod format;
mod reader;
mod varint;
mod writer;

pub use delta::{apply_delta, encode_delta};
pub use format::{EntryHeader, MAX_OBJECT_BYTES, MAX_PACK_OBJECTS, inflate_entry};
pub use reader::PackReader;
pub use varint::{read_varint, write_varint};
pub use writer::{
    write_pack_entry, write_pack_entry_header, write_pack_header, write_ref_delta_header,
};
