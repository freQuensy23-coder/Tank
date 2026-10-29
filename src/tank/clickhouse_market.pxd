cpdef object _canonical_numeric_text(object value):
    cdef str text
    cdef Py_ssize_t length, index, start, dot, first_nonzero
    cdef Py_UCS4 code
