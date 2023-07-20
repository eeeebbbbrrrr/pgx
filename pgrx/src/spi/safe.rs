use crate::{pg_sys, spi};

pub unsafe trait SpiSafe {}

unsafe impl SpiSafe for () {}
unsafe impl SpiSafe for String {}
unsafe impl SpiSafe for Vec<u8> {}
unsafe impl SpiSafe for alloc::ffi::CString {}
unsafe impl SpiSafe for bool {}
unsafe impl SpiSafe for char {}
unsafe impl SpiSafe for crate::datum::AnyNumeric {}
unsafe impl SpiSafe for crate::datum::Date {}
unsafe impl SpiSafe for crate::datum::Inet {}
unsafe impl SpiSafe for crate::datum::Interval {}
unsafe impl SpiSafe for crate::datum::Json {}
unsafe impl SpiSafe for crate::datum::JsonB {}
unsafe impl SpiSafe for crate::datum::Time {}
unsafe impl SpiSafe for crate::datum::TimeWithTimeZone {}
unsafe impl SpiSafe for crate::datum::Timestamp {}
unsafe impl SpiSafe for crate::datum::TimestampWithTimeZone {}
unsafe impl SpiSafe for crate::datum::Uuid {}
unsafe impl SpiSafe for f32 {}
unsafe impl SpiSafe for f64 {}
unsafe impl SpiSafe for i16 {}
unsafe impl SpiSafe for i32 {}
unsafe impl SpiSafe for i64 {}
unsafe impl SpiSafe for i8 {}
unsafe impl SpiSafe for pg_sys::BOX {}
unsafe impl SpiSafe for pg_sys::Oid {}
unsafe impl SpiSafe for pg_sys::Point {}
unsafe impl<T> SpiSafe for Vec<T> where T: SpiSafe {}
unsafe impl<const P: u32, const S: u32> SpiSafe for crate::datum::Numeric<P, S> {}
unsafe impl<T> SpiSafe for Option<T> where T: SpiSafe {}
unsafe impl SpiSafe for spi::SpiError {}
unsafe impl<T, E> SpiSafe for std::result::Result<T, E>
where
    T: SpiSafe,
    E: SpiSafe,
{
}
unsafe impl<A, B> SpiSafe for (A, B)
where
    A: SpiSafe,
    B: SpiSafe,
{
}
unsafe impl<A, B, C> SpiSafe for (A, B, C)
where
    A: SpiSafe,
    B: SpiSafe,
    C: SpiSafe,
{
}
