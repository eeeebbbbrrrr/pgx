use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Index;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::memcxt::PgMemoryContexts;
use crate::pg_sys::{self, PgOid};
use crate::prelude::*;
use crate::spi::SpiClient;

use super::{SpiError, SpiErrorCodes, SpiResult};

#[derive(Debug, Clone)]
struct Inner<'client> {
    // We borrow global state setup by the active SpiClient (pg_sys::SPI_tuptable).  We don't use
    // the client directly, but do need to make sure we don't outlive it, so here it is
    _client: PhantomData<&'client SpiClient>,

    // and this is that global state.  In SpiTupleTable::wrap(), this comes from whatever the current value of
    // `pg_sys::SPI_tuptable` happens to be.  Postgres may change where SPI_tuptable points
    // throughout the lifetime of an active SpiClient, but it doesn't mutate (or deallocate) what
    // it happens to point to.  This allows us to have multiple active SpiTupleTables
    // within a Spi connection.  Whatever this points to is freed via `pg_sys::SPI_freetuptable()`
    // when it is dropped.
    table: Option<NonNull<pg_sys::SPITupleTable>>,
    size: usize,

    // within the `SPITupleTable`, which row are we pointing at?
    current: Rc<RefCell<Option<usize>>>,
}

impl<'client> Inner<'client> {
    #[inline(always)]
    fn get_spi_tuptable(
        &self,
    ) -> SpiResult<(*mut pg_sys::SPITupleTable, *mut pg_sys::TupleDescData)> {
        let table = self.table.map(|table| table.as_ptr()).ok_or(SpiError::NoTupleTable)?;
        let tupdesc = unsafe {
            // SAFETY:  we just assured that `table` is not null
            table.as_mut().unwrap().tupdesc
        };
        Ok((table, tupdesc))
    }

    fn get<T: IntoDatum + FromDatum>(&self, ordinal: usize) -> SpiResult<Option<T>> {
        let (_, tupdesc) = self.get_spi_tuptable()?;
        let datum = self.get_datum_by_ordinal(ordinal)?;
        let is_null = datum.is_none();
        let datum = datum.unwrap_or_else(|| pg_sys::Datum::from(0));

        unsafe {
            // SAFETY:  we know the constraints around `datum` and `is_null` match because we
            // just got them from the underlying heap tuple
            Ok(T::try_from_datum_in_memory_context(
                PgMemoryContexts::CurrentMemoryContext
                    .parent()
                    .expect("parent memory context is absent"),
                datum,
                is_null,
                // SAFETY:  we know `self.tupdesc.is_some()` because an Ok return from
                // `self.get_datum_by_ordinal()` above already decided that for us
                pg_sys::SPI_gettypeid(tupdesc, ordinal as _),
            )?)
        }
    }

    /// Get a raw Datum from this HeapTuple by its ordinal position.
    ///
    /// The ordinal position is 1-based.
    ///
    /// # Errors
    ///
    /// If the specified ordinal is out of bounds a [`Error::SpiError(SpiError::NoAttribute)`] is returned
    /// If we have no backing tuple table a [`Error::NoTupleTable`] is returned
    fn get_datum_by_ordinal(&self, ordinal: usize) -> SpiResult<Option<pg_sys::Datum>> {
        self.check_ordinal_bounds(ordinal)?;

        let (table, tupdesc) = self.get_spi_tuptable()?;

        match self.current.borrow().as_ref() {
            None => Err(SpiError::InvalidPosition),
            Some(i) if *i >= self.size => Err(SpiError::InvalidPosition),
            Some(i) => unsafe {
                let heap_tuple = std::slice::from_raw_parts((*table).vals, self.size)[*i];
                let mut is_null = false;
                let datum = pg_sys::SPI_getbinval(heap_tuple, tupdesc, ordinal as _, &mut is_null);

                if is_null {
                    Ok(None)
                } else {
                    Ok(Some(datum))
                }
            },
        }
    }

    fn columns(&self) -> SpiResult<usize> {
        let (_, tupdesc) = self.get_spi_tuptable()?;
        // SAFETY:  we just got a valid tupdesc
        Ok(unsafe { (*tupdesc).natts as _ })
    }

    #[inline]
    fn check_ordinal_bounds(&self, ordinal: usize) -> SpiResult<()> {
        if ordinal < 1 || ordinal > self.columns()? {
            Err(SpiError::SpiError(SpiErrorCodes::NoAttribute))
        } else {
            Ok(())
        }
    }

    fn column_type_oid(&self, ordinal: usize) -> SpiResult<PgOid> {
        self.check_ordinal_bounds(ordinal)?;

        let (_, tupdesc) = self.get_spi_tuptable()?;
        unsafe {
            // SAFETY:  we just got a valid tupdesc
            let oid = pg_sys::SPI_gettypeid(tupdesc, ordinal as i32);
            Ok(PgOid::from(oid))
        }
    }

    fn column_name(&self, ordinal: usize) -> SpiResult<String> {
        self.check_ordinal_bounds(ordinal)?;
        let (_, tupdesc) = self.get_spi_tuptable()?;
        unsafe {
            // SAFETY:  we just got a valid tupdesc and we know ordinal is in bounds
            let name = pg_sys::SPI_fname(tupdesc, ordinal as i32);

            // SAFETY:  SPI_fname will have given us a properly allocated char* since we know
            // the specified ordinal is in bounds
            let str =
                CStr::from_ptr(name).to_str().expect("column name is not value UTF8").to_string();

            // SAFETY: we just asked Postgres to allocate name for us
            pg_sys::pfree(name as *mut _);
            Ok(str)
        }
    }

    fn column_ordinal<S: AsRef<str>>(&self, name: S) -> SpiResult<usize> {
        let (_, tupdesc) = self.get_spi_tuptable()?;
        unsafe {
            let name_cstr = CString::new(name.as_ref()).expect("name contained a null byte");
            let fnumber = pg_sys::SPI_fnumber(tupdesc, name_cstr.as_ptr());

            if fnumber == pg_sys::SPI_ERROR_NOATTRIBUTE {
                Err(SpiError::SpiError(SpiErrorCodes::NoAttribute))
            } else {
                Ok(fnumber as usize)
            }
        }
    }
}

#[derive(Debug)]
pub struct SpiTupleTable<'client> {
    inner: Inner<'client>,
    entries: Vec<Entry<'client>>,
}

impl<'client> SpiTupleTable<'client> {
    /// Wraps the current global `pg_sys::SPI_tuptable` as a new [`SpiTupleTable`] instance, with
    /// a lifetime tied to the specified [`SpiClient`].
    ///
    /// It is intended that that function be called to consume the globally-assigned results of
    /// SPI functions like [`pg_sys::SPI_execute`] and [`pg_sys::SPI_cursor_fetch`].
    ///
    /// # Panics
    ///
    /// This function will panic if the provided `last_spi_status_code` is not an "OK" code.
    pub(super) fn wrap(_client: &'client SpiClient, last_spi_status_code: i32) -> SpiResult<Self> {
        Spi::check_status(last_spi_status_code)?;

        unsafe {
            //
            // SAFETY:  The unsafeness here is that we're accessing static globals.  Fortunately,
            // Postgres is not multi-threaded so we're okay to do this
            //

            // different Postgres get the tuptable size different ways
            #[cfg(any(feature = "pg11", feature = "pg12"))]
            let size = pg_sys::SPI_processed as usize;

            #[cfg(not(any(feature = "pg11", feature = "pg12")))]
            let size = if pg_sys::SPI_tuptable.is_null() {
                pg_sys::SPI_processed as usize
            } else {
                (*pg_sys::SPI_tuptable).numvals as usize
            };

            let table = NonNull::new(pg_sys::SPI_tuptable);
            let natts =
                if let Some(ref table) = table { (*table.as_ref().tupdesc).natts } else { 0 };
            let current = Rc::new(RefCell::new(None));

            let inner = Inner { _client: PhantomData, table, size, current };
            let entries =
                (1..=natts as usize).map(|i| Entry { ordinal: i, inner: inner.clone() }).collect();
            let table = Self { inner, entries };

            // We should not leave pg_sys::SPI_tuptable pointing to something that we now control,
            // as we have no visibility into what (if anything) other Postgres internals or FFI calls
            // decide to do with it
            pg_sys::SPI_tuptable = std::ptr::null_mut();

            Ok(table)
        }
    }

    /// `SpiTupleTable`s are positioned before the start, for iteration purposes.
    ///
    /// This method moves the position to the first row.  If there are no rows, this
    /// method will silently return.
    pub fn first(self) -> Self {
        *self.inner.current.borrow_mut() = Some(0);
        self
    }

    /// Restore the state of iteration back to before the start.
    ///
    /// This is useful to iterate the table multiple times
    pub fn rewind(self) -> Self {
        *self.inner.current.borrow_mut() = None;
        self
    }

    /// Position the iteration state to the next row, returning `Some(&mut Self)`.  If at the end
    /// this function returns `None`.
    pub fn next(&mut self) -> Option<&mut Self> {
        let current = self.inner.current.borrow().map_or(0, |c| c + 1);
        *self.inner.current.borrow_mut() = Some(current);
        if current >= self.inner.size {
            None
        } else {
            Some(self)
        }
    }

    /// Takes a closure and creates an iterator which calls that closure on each row of this
    /// [`SpiTupleTuple`], starting at the current position.
    ///
    /// This is akin to the standard [`std::iter::Iterator::map()`] function.
    pub fn map<B, F>(self, f: F) -> Map<'client, F>
    where
        Self: Sized,
        F: FnMut(&mut SpiTupleTable<'_>) -> B,
    {
        Map { table: self, f }
    }

    /// How many rows were processed?
    pub fn len(&self) -> usize {
        self.inner.size
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_one<A: FromDatum + IntoDatum>(&self) -> SpiResult<Option<A>> {
        self.get(1)
    }

    pub fn get_two<A: FromDatum + IntoDatum, B: FromDatum + IntoDatum>(
        &self,
    ) -> SpiResult<(Option<A>, Option<B>)> {
        let a = self.get::<A>(1)?;
        let b = self.get::<B>(2)?;
        Ok((a, b))
    }

    pub fn get_three<
        A: FromDatum + IntoDatum,
        B: FromDatum + IntoDatum,
        C: FromDatum + IntoDatum,
    >(
        &self,
    ) -> SpiResult<(Option<A>, Option<B>, Option<C>)> {
        let a = self.get::<A>(1)?;
        let b = self.get::<B>(2)?;
        let c = self.get::<C>(3)?;
        Ok((a, b, c))
    }

    /// Get a typed value by its ordinal position.
    ///
    /// The ordinal position is 1-based.
    ///
    /// # Errors
    ///
    /// If the specified ordinal is out of bounds a [`Error::SpiError(SpiError::NoAttribute)`] is returned
    /// If we have no backing tuple table a [`Error::NoTupleTable`] is returned
    ///
    /// # Panics
    ///
    /// This function will panic there is no parent MemoryContext.  This is an incredibly unlikely
    /// situation.
    pub fn get<T: IntoDatum + FromDatum>(&self, ordinal: usize) -> SpiResult<Option<T>> {
        self.inner.get(ordinal)
    }

    /// Get a typed value by its name.
    ///
    /// # Errors
    ///
    /// If the specified name is invalid a [`Error::SpiError(SpiError::NoAttribute)`] is returned
    /// If we have no backing tuple table a [`Error::NoTupleTable`] is returned
    pub fn get_by_name<T: IntoDatum + FromDatum, S: AsRef<str>>(
        &self,
        name: S,
    ) -> SpiResult<Option<T>> {
        self.get(self.column_ordinal(name)?)
    }

    /// Returns the number of columns
    pub fn columns(&self) -> SpiResult<usize> {
        self.inner.columns()
    }

    /// Returns column type OID
    ///
    /// The ordinal position is 1-based
    pub fn column_type_oid(&self, ordinal: usize) -> SpiResult<PgOid> {
        self.inner.column_type_oid(ordinal)
    }

    /// Returns column name of the 1-based `ordinal` position
    ///
    /// # Errors
    ///
    /// Returns [`Error::SpiError(SpiError::NoAttribute)`] if the specified ordinal value is out of bounds
    /// If we have no backing tuple table a [`Error::NoTupleTable`] is returned
    ///
    /// # Panics
    ///
    /// This function will panic if the column name at the specified ordinal position is not also
    /// a valid UTF8 string.
    pub fn column_name(&self, ordinal: usize) -> SpiResult<String> {
        self.inner.column_name(ordinal)
    }

    /// Returns the ordinal (1-based position) of the specified column name
    ///
    /// # Errors
    ///
    /// Returns [`Error::SpiError(SpiError::NoAttribute)`] if the specified column name isn't found
    /// If we have no backing tuple table a [`Error::NoTupleTable`] is returned
    ///
    /// # Panics
    ///
    /// This function will panic if somehow the specified name contains a null byte.
    pub fn column_ordinal<S: AsRef<str>>(&self, name: S) -> SpiResult<usize> {
        self.inner.column_ordinal(name)
    }
}

impl Drop for SpiTupleTable<'_> {
    fn drop(&mut self) {
        unsafe {
            // SAFETY:  self.table was created by Postgres from whatever `pg_sys::SPI_tuptable` pointed
            // to at the time this SpiTupleTable was constructed
            if let Some(ptr) = self.inner.table.take() {
                pg_sys::SPI_freetuptable(ptr.as_ptr())
            }
        }
    }
}

impl<'table> Index<&str> for SpiTupleTable<'table> {
    type Output = Entry<'table>;

    /// Get the named attribute entry at the current position of this [`SpiTupleTable`].
    ///
    /// # Panics
    ///
    /// This method will panic if the specified attribute name is not found
    fn index(&self, name: &str) -> &Self::Output {
        let ordinal = self
            .column_ordinal(name)
            .unwrap_or_else(|_| panic!("no such attribute named `{}`", name));
        let index = ordinal - 1; // ordinals are 1-based, but we're not
        self.entries.get(index).unwrap()
    }
}

impl<'table> Index<usize> for SpiTupleTable<'table> {
    type Output = Entry<'table>;

    /// Get the numbered attribute entry at the current position of this [`SpiTupleTable`].
    ///
    /// The `index` argument ordinal position is 1-based.
    ///
    /// # Panics
    ///
    /// This method will panic if the specific ordinal position is out of bounds
    fn index(&self, ordinal: usize) -> &Self::Output {
        self.inner.check_ordinal_bounds(ordinal).expect("ordinal out of bounds");
        let index = ordinal - 1; // ordinals are 1-based, but we're not
        self.entries.get(index).unwrap()
    }
}

pub struct Map<'client, F> {
    table: SpiTupleTable<'client>,
    f: F,
}

impl<'client, B, F> Iterator for Map<'_, F>
where
    F: FnMut(&mut SpiTupleTable) -> B,
{
    type Item = B;

    #[inline]
    fn next(&mut self) -> Option<B> {
        self.table.next().map(&mut self.f)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.table.inner.size))
    }
}

/// A lightweight wrapper representing an attribute entry within a [`SpiHeapTuple`]
#[derive(Debug)]
pub struct Entry<'table> {
    ordinal: usize,
    inner: Inner<'table>,
}

impl<'table> Entry<'table> {
    /// Retrieve the value of this [`Entry`] from the current row of its associated table.
    #[inline]
    pub fn value<T: IntoDatum + FromDatum>(&self) -> SpiResult<Option<T>> {
        self.inner.get(self.ordinal)
    }
}
