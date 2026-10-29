cdef int _fixed_decimal_text_metadata(object value):
    cdef str text
    cdef Py_ssize_t length, index, dot, digits, significant, exponent
    cdef Py_UCS4 code
