import cython

@cython.locals(
    text=str,
    length=cython.Py_ssize_t,
    index=cython.Py_ssize_t,
    start=cython.Py_ssize_t,
    dot=cython.Py_ssize_t,
    first_nonzero=cython.Py_ssize_t,
    code=cython.int,
)
cpdef object _canonical_numeric_text(object value)
