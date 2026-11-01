import cython
from libc.stdint cimport uint64_t

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

@cython.locals(index=cython.Py_ssize_t, count=cython.Py_ssize_t, records=list, template=dict, record=dict)
cdef api object _materialize_field_columns(object fields)

@cython.locals(value=uint64_t, part=uint64_t, byte=cython.uint, shift=cython.uint)
cdef api object _binary_varuint(bytes body, Py_ssize_t position)

@cython.locals(size=object, end=cython.Py_ssize_t, value=object)
cdef api object _binary_string(bytes body, Py_ssize_t position)

@cython.locals(value=object, text=object, count=object, _index=cython.Py_ssize_t, values=list, compatible=cython.bint)
cdef api object _binary_value(bytes body, Py_ssize_t position, int code, dict timestamps)

@cython.locals(
    position=cython.Py_ssize_t,
    count=object,
    minimum=cython.Py_ssize_t,
    records=list,
    record=list,
    timestamps=dict,
    _index=cython.Py_ssize_t,
    code=cython.int,
    nullable=cython.bint,
    marker=cython.uint,
    supported=cython.bint,
    compatible=cython.bint,
)
cdef api object _decode_sampled_binary(bytes body, object schema)
