.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. default-domain:: cpp
.. highlight:: cpp
.. cpp:namespace:: arrow::compute

=================
Compute Functions
=================

The generic Compute API
=======================

.. TODO: describe API and how to invoke compute functions

Functions and function registry
-------------------------------

Functions represent logical compute operations over inputs of possibly
varying types.  Internally, a function is implemented by one or several
"kernels", depending on the concrete input types (for example, a function
adding values from two inputs can have different kernels depending on
whether the inputs are integral or floating-point).

Functions are stored in a global :class:`FunctionRegistry` where
they can be looked up by name.

Input shapes
------------

Computation inputs are represented as a general :class:`Datum` class,
which is a tagged union of several shapes of data such as :class:`Scalar`,
:class:`Array` and :class:`ChunkedArray`.  Many compute functions support
both array (chunked or not) and scalar inputs, however some will mandate
either.  For example, the ``fill_null`` function requires its second input
to be a scalar, while ``sort_indices`` requires its first and only input to
be an array.

Invoking functions
------------------

Compute functions can be invoked by name using
:func:`arrow::compute::CallFunction`::

   std::shared_ptr<arrow::Array> numbers_array = ...;
   std::shared_ptr<arrow::Scalar> increment = ...;
   arrow::Datum incremented_datum;

   ARROW_ASSIGN_OR_RAISE(incremented_datum,
                         arrow::compute::CallFunction("add", {numbers_array, increment}));
   std::shared_ptr<Array> incremented_array = std::move(incremented_datum).array();

(note this example uses implicit conversion from ``std::shared_ptr<Array>``
to ``Datum``)

Many compute functions are also available directly as concrete APIs, here
:func:`arrow::compute::Add`::

   std::shared_ptr<arrow::Array> numbers_array = ...;
   std::shared_ptr<arrow::Scalar> increment = ...;
   arrow::Datum incremented_datum;

   ARROW_ASSIGN_OR_RAISE(incremented_datum,
                         arrow::compute::Add(numbers_array, increment));
   std::shared_ptr<Array> incremented_array = std::move(incremented_datum).array();

Some functions accept or require an options structure that determines the
exact semantics of the function::

   MinMaxOptions options;
   options.null_handling = MinMaxOptions::OUTPUT_NULL;

   std::shared_ptr<arrow::Array> array = ...;
   arrow::Datum minmax_datum;

   ARROW_ASSIGN_OR_RAISE(minmax_datum,
                         arrow::compute::CallFunction("minmax", {array}, &options));

   // Unpack struct scalar result (a two-field {"min", "max"} scalar)
   const auto& minmax_scalar = \
         static_cast<const arrow::StructScalar&>(*minmax_datum.scalar());
   const auto min_value = minmax_scalar.value[0];
   const auto max_value = minmax_scalar.value[1];

.. seealso::
   :doc:`Compute API reference <api/compute>`


Available functions
===================

Type categories
---------------

To avoid exhaustively listing supported types, the tables below use a number
of general type categories:

* "Numeric": Integer types (Int8, etc.) and Floating-point types (Float32,
  Float64, sometimes Float16).  Some functions also accept Decimal128 input.

* "Temporal": Date types (Date32, Date64), Time types (Time32, Time64),
  Timestamp, Duration, Interval.

* "Binary-like": Binary, LargeBinary, sometimes also FixedSizeBinary.

* "String-like": String, LargeString.

* "List-like": List, LargeList, sometimes also FixedSizeList.

If you are unsure whether a function supports a concrete input type, we
recommend you try it out.  Unsupported input types return a ``TypeError``
:class:`Status`.

Aggregations
------------

+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| Function name            | Arity      | Input types        | Output type           | Options class                              |
+==========================+============+====================+=======================+============================================+
| count                    | Unary      | Any                | Scalar Int64          | :struct:`CountOptions`                     |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| mean                     | Unary      | Numeric            | Scalar Float64        |                                            |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| minmax                   | Unary      | Numeric            | Scalar Struct  (1)    | :struct:`MinMaxOptions`                    |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| sum                      | Unary      | Numeric            | Scalar Numeric (2)    |                                            |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+

Notes:

* \(1) Output is a ``{"min": input type, "max": input type}`` Struct

* \(2) Output is Int64, UInt64 or Float64, depending on the input type


Element-wise ("scalar") functions
---------------------------------

All element-wise functions accept both arrays and scalars as input.  The
semantics for unary functions are as follow:

* scalar inputs produce a scalar output
* array inputs produce an array output

Binary functions have the following semantics (which is sometimes called
"broadcasting" in other systems such as NumPy):

* ``(scalar, scalar)`` inputs produce a scalar output
* ``(array, array)`` inputs produce an array output (and both inputs must
  be of the same length)
* ``(scalar, array)`` and ``(array, scalar)`` produce an array output.
  The scalar input is handled as if it were an array of the same length N
  as the other input, with the same value repeated N times.

Arithmetic functions
~~~~~~~~~~~~~~~~~~~~

These functions expect two inputs of the same type and apply a given binary
operation to each pair of elements gathered from the inputs.  If any of the
input elements in a pair is null, the corresponding output element is null.

The default variant of these functions does not detect overflow (the result
then typically wraps around).  Each function is also available in an
overflow-checking variant, suffixed ``_checked``, which returns
an ``Invalid`` :class:`Status` when overflow is detected.

+--------------------------+------------+--------------------+---------------------+
| Function name            | Arity      | Input types        | Output type         |
+==========================+============+====================+=====================+
| add                      | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| add_checked              | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| multiply                 | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| multiply_checked         | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| subtract                 | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+
| subtract_checked         | Binary     | Numeric            | Numeric             |
+--------------------------+------------+--------------------+---------------------+

Comparisons
~~~~~~~~~~~

Those functions expect two inputs of the same type and apply a given
comparison operator.  If any of the input elements in a pair is null,
the corresponding output element is null.

+--------------------------+------------+---------------------------------------------+---------------------+
| Function names           | Arity      | Input types                                 | Output type         |
+==========================+============+=============================================+=====================+
| equal, not_equal         | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
+--------------------------+------------+---------------------------------------------+---------------------+
| greater, greater_equal,  | Binary     | Numeric, Temporal, Binary- and String-like  | Boolean             |
| less, less_equal         |            |                                             |                     |
+--------------------------+------------+---------------------------------------------+---------------------+

Logical functions
~~~~~~~~~~~~~~~~~~

The normal behaviour for these functions is to emit a null if any of the
inputs is null (similar to the semantics of ``NaN`` in floating-point
computations).

Some of them are also available in a `Kleene logic`_ variant (suffixed
``_kleene``) where null is taken to mean "undefined".  This is the
interpretation of null used in SQL systems as well as R and Julia,
for example.

For the Kleene logic variants, therefore:

* "true AND null", "null AND true" give "null" (the result is undefined)
* "true OR null", "null OR true" give "true"
* "false AND null", "null AND false" give "false"
* "false OR null", "null OR false" give "null" (the result is undefined)

+--------------------------+------------+--------------------+---------------------+
| Function name            | Arity      | Input types        | Output type         |
+==========================+============+====================+=====================+
| and                      | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| and_kleene               | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| invert                   | Unary      | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| or                       | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| or_kleene                | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+
| xor                      | Binary     | Boolean            | Boolean             |
+--------------------------+------------+--------------------+---------------------+

.. _Kleene logic: https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics

String functions
~~~~~~~~~~~~~~~~

+--------------------------+------------+--------------------+---------------------+---------+
| Function name            | Arity      | Input types        | Output type         | Notes   |
+==========================+============+====================+=====================+=========+
| ascii_length             | Unary      | String-like        | Int32 or Int64      | \(1)    |
+--------------------------+------------+--------------------+---------------------+---------+
| ascii_lower              | Unary      | String-like        | String-like         | \(2)    |
+--------------------------+------------+--------------------+---------------------+---------+
| ascii_upper              | Unary      | String-like        | String-like         | \(2)    |
+--------------------------+------------+--------------------+---------------------+---------+
| utf8_lower               | Unary      | String-like        | String-like         | \(3)    |
+--------------------------+------------+--------------------+---------------------+---------+
| utf8_upper               | Unary      | String-like        | String-like         | \(3)    |
+--------------------------+------------+--------------------+---------------------+---------+

* \(1) Output is the physical length in bytes of each input element.  Output
  type is Int32 for String, Int64 for LargeString.

* \(2) Each ASCII character in the input is converted to lowercase or
  uppercase.  Non-ASCII characters are left untouched.

* \(3) Each UTF8-encoded character in the input is converted to lowercase or
  uppercase.

Containment tests
~~~~~~~~~~~~~~~~~

+--------------------------+------------+------------------------------------+---------------+----------------------------------------+
| Function name            | Arity      | Input types                        | Output type   | Options class                          |
+==========================+============+====================================+===============+========================================+
| binary_contains_exact    | Unary      | String-like                        | Boolean (1)   | :struct:`BinaryContainsExactOptions`   |
+--------------------------+------------+------------------------------------+---------------+----------------------------------------+
| isin                     | Unary      | Boolean, Null, Numeric, Temporal,  | Boolean (2)   | :struct:`SetLookupOptions`             |
|                          |            | Binary- and String-like            |               |                                        |
+--------------------------+------------+------------------------------------+---------------+----------------------------------------+
| match                    | Unary      | Boolean, Null, Numeric, Temporal,  | Int32 (3)     | :struct:`SetLookupOptions`             |
|                          |            | Binary- and String-like            |               |                                        |
+--------------------------+------------+------------------------------------+---------------+----------------------------------------+

* \(1) Output is true iff :member:`BinaryContainsExactOptions::pattern`
  is a substring of the corresponding input element.

* \(2) Output is true iff the corresponding input element is equal to one
  of the elements in :member:`SetLookupOptions::value_set`.

* \(3) Output is the index of the corresponding input element in
  :member:`SetLookupOptions::value_set`, if found there.  Otherwise,
  output is null.

Structural transforms
~~~~~~~~~~~~~~~~~~~~~

.. XXX (this category is a bit of a hodgepodge)

+--------------------------+------------+---------------------------------------+---------------------+---------+
| Function name            | Arity      | Input types                           | Output type         | Notes   |
+==========================+============+=======================================+=====================+=========+
| fill_null                | Binary     | Boolean, Null, Numeric, Temporal      | Boolean             | \(1)    |
+--------------------------+------------+---------------------------------------+---------------------+---------+
| is_null                  | Unary      | Any                                   | Boolean             | \(2)    |
+--------------------------+------------+---------------------------------------+---------------------+---------+
| is_valid                 | Unary      | Any                                   | Boolean             | \(2)    |
+--------------------------+------------+---------------------------------------+---------------------+---------+
| list_value_lengths       | Unary      | List-like                             | Int32 or Int64      | \(4)    |
+--------------------------+------------+---------------------------------------+---------------------+---------+

* \(1) First input must be an array, second input a scalar of the same type.
  Output is an array of the same type as the inputs, and with the same values
  as the first input, except for nulls replaced with the second input value.

* \(2) Output is true iff the corresponding input element is non-null.

* \(3) Output is true iff the corresponding input element is null.

* \(4) Each output element is the length of the corresponding input element
  (null if input is null).  Output type is Int32 for List, Int64 for LargeList.

Conversions
~~~~~~~~~~~

A general conversion function named ``cast`` is provided which accepts a large
number of input and output types.  The type to cast to can be passed in a
:struct:`CastOptions` instance.  As an alternative, the same service is
provided by a concrete function :func:`~arrow::compute::Cast`.

+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| Function name            | Arity      | Input types        | Output type           | Options class                              |
+==========================+============+====================+=======================+============================================+
| cast                     | Unary      | Many               | Variable              | :struct:`CastOptions`                      |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+
| strptime                 | Unary      | String-like        | Timestamp             | :struct:`StrptimeOptions`                  |
+--------------------------+------------+--------------------+-----------------------+--------------------------------------------+

The conversions available with ``cast`` are listed below.  In all cases, a
null input value is converted into a null output value.

**Truth value extraction**

+-----------------------------+------------------------------------+--------------+
| Input type                  | Output type                        | Notes        |
+=============================+====================================+==============+
| Binary- and String-like     | Boolean                            | \(1)         |
+-----------------------------+------------------------------------+--------------+
| Numeric                     | Boolean                            | \(2)         |
+-----------------------------+------------------------------------+--------------+

* \(1) Output is true iff the corresponding input value has non-zero length.

* \(2) Output is true iff the corresponding input value is non-zero.

**Same-kind conversion**

+-----------------------------+------------------------------------+--------------+
| Input type                  | Output type                        | Notes        |
+=============================+====================================+==============+
| Int32                       | 32-bit Temporal                    | \(1)         |
+-----------------------------+------------------------------------+--------------+
| Int64                       | 64-bit Temporal                    | \(1)         |
+-----------------------------+------------------------------------+--------------+
| (Large)Binary               | (Large)String                      | \(2)         |
+-----------------------------+------------------------------------+--------------+
| (Large)String               | (Large)Binary                      | \(3)         |
+-----------------------------+------------------------------------+--------------+
| Numeric                     | Numeric                            | \(4) \(5)    |
+-----------------------------+------------------------------------+--------------+
| 32-bit Temporal             | Int32                              | \(1)         |
+-----------------------------+------------------------------------+--------------+
| 64-bit Temporal             | Int64                              | \(1)         |
+-----------------------------+------------------------------------+--------------+
| Temporal                    | Temporal                           | \(4) \(5)    |
+-----------------------------+------------------------------------+--------------+

* \(1) No-operation cast: the raw values are kept identical, only
  the type is changed.

* \(2) Validates the contents if :member:`CastOptions::allow_invalid_utf8`
  is false.

* \(3) No-operation cast: only the type is changed.

* \(4) Overflow and truncation checks are enabled depending on
  the given :struct:`CastOptions`.

* \(5) Not all such casts have been implemented.

**String representations**

+-----------------------------+------------------------------------+---------+
| Input type                  | Output type                        | Notes   |
+=============================+====================================+=========+
| Boolean                     | String-like                        |         |
+-----------------------------+------------------------------------+---------+
| Numeric                     | String-like                        |         |
+-----------------------------+------------------------------------+---------+

**Generic conversions**

+-----------------------------+------------------------------------+---------+
| Input type                  | Output type                        | Notes   |
+=============================+====================================+=========+
| Dictionary                  | Dictionary value type              | \(1)    |
+-----------------------------+------------------------------------+---------+
| Extension                   | Extension storage type             |         |
+-----------------------------+------------------------------------+---------+
| List-like                   | List-like                          | \(2)    |
+-----------------------------+------------------------------------+---------+
| Null                        | Any                                |         |
+-----------------------------+------------------------------------+---------+

* \(1) The dictionary indices are unchanged, the dictionary values are
  cast from the input value type to the output value type (if a conversion
  is available).

* \(2) The list offsets are unchanged, the list values are cast from the
  input value type to the output value type (if a conversion is
  available).


Array-wise ("vector") functions
-------------------------------

Associative transforms
~~~~~~~~~~~~~~~~~~~~~~

+--------------------------+------------+------------------------------------+----------------------------+
| Function name            | Arity      | Input types                        | Output type                |
+==========================+============+====================================+============================+
| dictionary_encode        | Unary      | Boolean, Null, Numeric,            | Dictionary (1)             |
|                          |            | Temporal, Binary- and String-like  |                            |
+--------------------------+------------+------------------------------------+----------------------------+
| unique                   | Unary      | Boolean, Null, Numeric,            | Input type (2)             |
|                          |            | Temporal, Binary- and String-like  |                            |
+--------------------------+------------+------------------------------------+----------------------------+
| value_counts             | Unary      | Boolean, Null, Numeric,            | Input type (3)             |
|                          |            | Temporal, Binary- and String-like  |                            |
+--------------------------+------------+------------------------------------+----------------------------+

* \(1) Output is ``Dictionary(Int32, input type)``.

* \(2) Duplicates are removed from the output while the original order is
  maintained.

* \(3) Output is a ``{"values": input type, "counts": Int64}`` Struct.
  Each output element corresponds to a unique value in the input, along
  with the number of times this value has appeared.

Selections
~~~~~~~~~~

These functions select a subset of the first input defined by the second input.

+-----------------+------------+---------------+--------------+------------------+-------------------------+-------------+
| Function name   | Arity      | Input type 1  | Input type 2 | Output type      | Options class           | Notes       |
+=================+============+===============+==============+==================+=========================+=============+
| filter          | Binary     | Any (1)       | Boolean      | Input type 1     | :struct:`FilterOptions` | \(2)        |
+-----------------+------------+---------------+--------------+------------------+-------------------------+-------------+
| take            | Binary     | Any (1)       | Integer      | Input type 1     | :struct:`TakeOptions`   | \(3)        |
+-----------------+------------+---------------+--------------+------------------+-------------------------+-------------+

* \(1) Unions are unsupported.

* \(2) Each element in input 1 is appended to the output iff the corresponding
  element in input 2 is true.

* \(3) For each element *i* in input 2, the *i*'th element in input 1 is
  appended to the output.

Sorts and partitions
~~~~~~~~~~~~~~~~~~~~

In these functions, nulls are considered greater than any other value
(they will be sorted or partitioned at the end of the array).

+-----------------------+------------+-------------------------+-------------------+--------------------------------+-------------+
| Function name         | Arity      | Input types             | Output type       | Options class                  | Notes       |
+=======================+============+=========================+===================+================================+=============+
| partition_indices     | Unary      | Binary- and String-like | UInt64            | :struct:`PartitionOptions`     | \(1) \(3)   |
+-----------------------+------------+-------------------------+-------------------+--------------------------------+-------------+
| partition_indices     | Unary      | Numeric                 | UInt64            | :struct:`PartitionOptions`     | \(1)        |
+-----------------------+------------+-------------------------+-------------------+--------------------------------+-------------+
| sort_indices          | Unary      | Binary- and String-like | UInt64            |                                | \(2) \(3)   |
+-----------------------+------------+-------------------------+-------------------+--------------------------------+-------------+
| sort_indices          | Unary      | Numeric                 | UInt64            |                                | \(2)        |
+-----------------------+------------+-------------------------+-------------------+--------------------------------+-------------+

* \(1) The output is an array of indices into the input array, that define
  a partition around the *N*'th input array element in sorted order.  *N* is
  given in :member:`PartitionOptions::pivot`.

* \(2) The output is an array of indices into the input array, that define
  a non-stable sort of the input array.

* \(3) Input values are ordered lexicographically as bytestrings (even
  for String arrays).


Structural transforms
~~~~~~~~~~~~~~~~~~~~~

+--------------------------+------------+--------------------+---------------------+---------+
| Function name            | Arity      | Input types        | Output type         | Notes   |
+==========================+============+====================+=====================+=========+
| list_flatten             | Unary      | List-like          | List value type     | \(1)    |
+--------------------------+------------+--------------------+---------------------+---------+
| list_parent_indices      | Unary      | List-like          | Int32 or Int64      | \(2)    |
+--------------------------+------------+--------------------+---------------------+---------+

* \(1) The top level of nesting is removed: all values in the list child array,
  including nulls, are appended to the output.  However, nulls in the parent
  list array are discarded.

* \(2) For each value in the list child array, the index at which it is found
  in the list array is appended to the output.  Nulls in the parent list array
  are discarded.
