import cython

cdef int _fixed_decimal_text_metadata(object value):
    cdef str text
    cdef Py_ssize_t length, index, dot, digits, significant, exponent
    cdef Py_UCS4 code

@cython.locals(inputs=dict, memo=dict, precisions=dict)
cdef api object _scalar_decimal(object v)

@cython.locals(times=dict, inputs=dict, memo=dict)
cdef api object _row_time(object row)

@cython.locals(inputs=dict, memo=dict)
cdef api object _row_available(object row, object snapshot)

@cython.locals(inputs=dict, conversions=dict, metadata=dict, scalars=dict, precisions=dict)
cdef api object _fixed_decimal_column(object column)
