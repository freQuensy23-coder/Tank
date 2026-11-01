import cython
from libc.stdint cimport int64_t, uint64_t

cdef extern from *:
    """
    typedef unsigned __int128 tank_history_wide;
    """
    ctypedef unsigned long long tank_history_wide

ctypedef const int64_t[::1] _HistoryUnits
ctypedef const Py_ssize_t[::1] _HistoryPositions

cdef object _MONEY_RADIX

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

cdef api object _as_dec(object value)
cdef api object _when(object row)
cdef api object _can_admit(object row, object snapshot)

@cython.locals(
    view=object[::1],
    saved=object[::1],
    index=cython.Py_ssize_t,
    offset=cython.Py_ssize_t,
)
cdef api object _validated_integer_units(object values)

@cython.locals(
    numerator=uint64_t,
    denominator=uint64_t,
    left=uint64_t,
    right=uint64_t,
    exponent=cython.int,
    shift=cython.int,
    comparison=tank_history_wide,
    scaled_numerator=tank_history_wide,
    scaled_denominator=tank_history_wide,
    quotient=uint64_t,
    remainder=tank_history_wide,
    twice=tank_history_wide,
    value=cython.double,
)
cdef api double _rounded_integer_change(uint64_t current, uint64_t old) except? -1

@cython.locals(
    units_read=_HistoryUnits,
    previous_read=_HistoryPositions,
    positions_read=_HistoryPositions,
    result=cython.double[::1],
    index=cython.Py_ssize_t,
    position=cython.Py_ssize_t,
    prior=cython.Py_ssize_t,
    current=int64_t,
    old=int64_t,
    denominator=int64_t,
    inexact=cython.bint,
)
cdef api object _integer_return_values(object units, object previous, object positions)
