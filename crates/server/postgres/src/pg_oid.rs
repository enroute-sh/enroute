//! [`PgOid`] — a local newtype carrying sqlx's Postgres `bytea` codec for
//! `gix_hash::ObjectId`.

use gix_hash::ObjectId;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgValueRef};
use sqlx::{Encode, Postgres};

const OID_LEN: usize = 20;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PgOid(ObjectId);

impl From<ObjectId> for PgOid {
    fn from(oid: ObjectId) -> Self {
        Self(oid)
    }
}

impl From<PgOid> for ObjectId {
    fn from(oid: PgOid) -> Self {
        oid.0
    }
}

// Through `Vec<u8>` because it is the public way to name `bytea`:
// `PgTypeInfo::BYTEA` is crate-private to sqlx.
impl sqlx::Type<Postgres> for PgOid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Vec<u8> as sqlx::Type<Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Vec<u8> as sqlx::Type<Postgres>>::compatible(ty)
    }
}

impl Encode<'_, Postgres> for PgOid {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&[u8] as Encode<Postgres>>::encode(self.0.as_slice(), buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for PgOid {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let bytes = <&[u8] as sqlx::Decode<Postgres>>::decode(value)?;
        if bytes.len() != OID_LEN {
            return Err(format!("metadata store: oid length {} != {OID_LEN}", bytes.len()).into());
        }
        Ok(Self(ObjectId::from_bytes_or_panic(bytes)))
    }
}
