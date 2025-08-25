use std::ffi::c_void;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::Schema;
use pyo3::exceptions::PyValueError;
use pyo3::ffi::PyCapsule_New;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Creates a PyCapsule containing an ArrowSchema
pub fn schema_to_pycapsule<'py>(
    py: Python<'py>,
    schema: &Schema,
) -> PyResult<Bound<'py, PyCapsule>> {
    let schema_ptr = Box::into_raw(Box::new(FFI_ArrowSchema::try_from(schema).map_err(
        |e| PyValueError::new_err(format!("Failed to convert schema to FFI format: {}", e)),
    )?));

    // SAFETY: `schema_ptr` points to a valid `FFI_ArrowSchema` allocated by `Box`,
    // and we provide a proper release callback to clean it up
    let capsule_ptr = unsafe {
        PyCapsule_New(
            schema_ptr as *mut c_void,
            c"arrow_schema".as_ptr(),
            Some(release_arrow_schema_capsule),
        )
    };

    if capsule_ptr.is_null() {
        // Clean up if capsule creation failed
        // SAFETY: `schema_ptr` was created by `Box::into_raw` above, so we own it and can safely drop it
        unsafe {
            drop(Box::from_raw(schema_ptr));
        }
        return Err(PyValueError::new_err("Failed to create schema capsule"));
    }

    // SAFETY: `capsule_ptr` is a valid non-null `PyObject*` returned by `PyCapsule_New`
    Ok(unsafe { Bound::from_owned_ptr(py, capsule_ptr) }.downcast_into()?)
}

/// Creates a PyCapsule containing an ArrowArray
pub fn array_to_pycapsule<'py>(
    py: Python<'py>,
    array: &dyn Array,
) -> PyResult<Bound<'py, PyCapsule>> {
    let array_ptr = Box::into_raw(Box::new(FFI_ArrowArray::new(&array.to_data())));

    // SAFETY: `array_ptr` points to a valid `FFI_ArrowArray` allocated by `Box`,
    // and we provide a proper release callback to clean it up
    let capsule_ptr = unsafe {
        PyCapsule_New(
            array_ptr as *mut c_void,
            c"arrow_array".as_ptr(),
            Some(release_arrow_array_capsule),
        )
    };

    if capsule_ptr.is_null() {
        // Clean up if capsule creation failed
        // SAFETY: `array_ptr` was created by `Box::into_raw` above, so we own it and can safely drop it
        unsafe {
            drop(Box::from_raw(array_ptr));
        }
        return Err(PyValueError::new_err("Failed to create array capsule"));
    }

    // SAFETY: `capsule_ptr` is a valid non-null `PyObject*` returned by `PyCapsule_New`
    Ok(unsafe { Bound::from_owned_ptr(py, capsule_ptr) }.downcast_into()?)
}

/// Creates a tuple of (schema_capsule, array_capsule) from a RecordBatch
pub fn record_batch_to_pycapsules<'py>(
    py: Python<'py>,
    record_batch: &RecordBatch,
) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
    let schema_capsule = schema_to_pycapsule(py, record_batch.schema().as_ref())?;

    // For record batches, we need to convert to a struct array
    let struct_array = StructArray::from(record_batch.clone());
    let array_capsule = array_to_pycapsule(py, &struct_array)?;

    Ok((schema_capsule, array_capsule))
}

/// Release callback for schema capsules
unsafe extern "C" fn release_arrow_schema_capsule(capsule: *mut pyo3::ffi::PyObject) {
    if capsule.is_null() {
        return;
    }

    // SAFETY: This function is called by Python when the capsule is being destroyed.
    // The capsule was created with a valid schema pointer, so we can safely retrieve and drop it.
    unsafe {
        let schema_ptr = pyo3::ffi::PyCapsule_GetPointer(capsule, c"arrow_schema".as_ptr())
            as *mut FFI_ArrowSchema;

        if !schema_ptr.is_null() {
            // SAFETY: `schema_ptr` was originally created by `Box::into_raw`, so it's safe to reconstruct the `Box`
            drop(Box::from_raw(schema_ptr));
        }
    }
}

/// Release callback for array capsules
unsafe extern "C" fn release_arrow_array_capsule(capsule: *mut pyo3::ffi::PyObject) {
    if capsule.is_null() {
        return;
    }

    // SAFETY: This function is called by Python when the capsule is being destroyed.
    // The capsule was created with a valid array pointer, so we can safely retrieve and drop it.
    unsafe {
        let array_ptr = pyo3::ffi::PyCapsule_GetPointer(capsule, c"arrow_array".as_ptr())
            as *mut FFI_ArrowArray;

        if !array_ptr.is_null() {
            // SAFETY: `array_ptr` was originally created by `Box::into_raw`, so it's safe to reconstruct the `Box`
            drop(Box::from_raw(array_ptr));
        }
    }
}
