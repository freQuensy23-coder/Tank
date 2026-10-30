import cython

@cython.locals(
    text=str,
    length=cython.Py_ssize_t,
    index=cython.Py_ssize_t,
    dot=cython.Py_ssize_t,
    digits=cython.Py_ssize_t,
    significant=cython.Py_ssize_t,
    exponent=cython.Py_ssize_t,
    code=cython.Py_UCS4,
)
cdef int _fixed_decimal_text_metadata(object value)

@cython.locals(inputs=dict, memo=dict, precisions=dict)
cdef api object _scalar_decimal(object v)

@cython.locals(times=dict, inputs=dict, memo=dict, owners=dict)
cdef api object _row_time(object row)

@cython.locals(inputs=dict, memo=dict)
cdef api object _row_available(object row, object snapshot)

@cython.locals(
    inputs=dict,
    metadata=dict,
    scalars=dict,
    precisions=dict,
    low=cython.int,
    high=cython.int,
    prepared=cython.int,
    exponent=cython.int,
    significant=cython.int,
    adjusted=cython.int,
    index=cython.Py_ssize_t,
)
cdef api object _fixed_decimal_column(object column)

@cython.locals(
    inputs=dict,
    memo=dict,
    owners=dict,
    descriptors=dict,
    arrays=list,
    values=list,
    low=cython.int,
    high=cython.int,
    exponent=cython.int,
    adjusted=cython.int,
    descriptor=cython.int,
)
cdef api object _prepared_history_decimal_arrays(object columns, object summands, object metadata)
