import cython

@cython.locals(dictionary=dict, sequence=list, result=dict, items=list)
cdef object _owned_json(object value, object missing, Py_ssize_t depth)
