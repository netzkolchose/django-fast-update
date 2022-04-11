import os
import sys
from threading import Thread
from io import BytesIO
from binascii import b2a_hex
from operator import attrgetter
from decimal import Decimal as Decimal
from datetime import date, datetime, timedelta, time as dt_time
from json import dumps
from uuid import UUID
from django.db import connections, transaction, models
from django.db.models.fields.related import RelatedField
from django.contrib.postgres.fields import (HStoreField, ArrayField, IntegerRangeField,
    BigIntegerRangeField, DecimalRangeField, DateTimeRangeField, DateRangeField)
from psycopg2.extras import Range

# typings imports
from django.db.backends.utils import CursorWrapper
from typing import Any, BinaryIO, Callable, Dict, Generic, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar, Union
from typing import cast

EncoderProto = TypeVar("EncoderProto", bound=Callable[[Any, str, List[Any]], Any])

if sys.version_info > (3, 7):
    from typing import Protocol
    class FieldEncoder(Protocol[EncoderProto]):
        array_escape: bool
        __call__: EncoderProto
else:
    class FieldEncoder(Generic[EncoderProto]):
        array_escape: bool
        __call__: EncoderProto

# TODO: proper typing for lazy encoders and lazy container


# postgres connection encodings mapped to python
# taken from https://www.postgresql.org/docs/current/multibyte.html
# python counterparts shamelessly taken from psycopg3 :)
CONNECTION_ENCODINGS = {
    'BIG5': 'big5',
    'EUC_CN': 'gb2312',
    'EUC_JP': 'euc_jp',
    'EUC_JIS_2004': 'euc_jis_2004',
    'EUC_KR': 'euc_kr',
    #'EUC_TW': not supported
    'GB18030': 'gb18030',
    'GBK': 'gbk',
    'ISO_8859_5': 'iso8859-5',
    'ISO_8859_6': 'iso8859-6',
    'ISO_8859_7': 'iso8859-7',
    'ISO_8859_8': 'iso8859-8',
    'JOHAB': 'johab',
    'KOI8R': 'koi8-r',
    'KOI8U': 'koi8-u',
    'LATIN1': 'iso8859-1',
    'LATIN2': 'iso8859-2',
    'LATIN3': 'iso8859-3',
    'LATIN4': 'iso8859-4',
    'LATIN5': 'iso8859-9',
    'LATIN6': 'iso8859-10',
    'LATIN7': 'iso8859-13',
    'LATIN8': 'iso8859-14',
    'LATIN9': 'iso8859-15',
    'LATIN10': 'iso8859-16',
    #'MULE_INTERNAL': not supported
    'SJIS': 'shift_jis',
    'SHIFT_JIS_2004': 'shift_jis_2004',
    'SQL_ASCII': 'ascii',
    'UHC': 'cp949',
    'UTF8': 'utf-8',
    'WIN866': 'cp866',
    'WIN874': 'cp874',
    'WIN1250': 'cp1250',
    'WIN1251': 'cp1251',
    'WIN1252': 'cp1252',
    'WIN1253': 'cp1253',
    'WIN1254': 'cp1254',
    'WIN1255': 'cp1255',
    'WIN1256': 'cp1256',
    'WIN1257': 'cp1257',
    'WIN1258': 'cp1258',
}


# NULL placeholder for COPY FROM
NULL = '\\N'
# SQL NULL
SQL_NULL = 'NULL'
# lazy placeholder: NUL - not allowed in postgres data (cannot slip through)
LAZY_PLACEHOLDER = '\x00'
LAZY_PLACEHOLDER_BYTE = b'\x00'


def text_escape(v: str) -> str:
    """
    Escape str-like data for postgres' TEXT format.

    This does all basic TEXT escaping. For nested types as of hstore
    or in arrays, array_escape must be applied on top.
    """
    return (v.replace('\\', '\\\\')
        .replace('\b', '\\b').replace('\f', '\\f').replace('\n', '\\n')
        .replace('\r', '\\r').replace('\t', '\\t').replace('\v', '\\v'))



# Rules for nested types:
# - a backslash in nested strings needs 2x2 escaping, e.g. \ --> \\ --> \\\\
# - nested str values may need explicit quoting to avoid collisions
#   --> simply always quote
# - due to quoting, " needs \\" escape in nested strings
# - null value is again sql NULL (handled in nesting encoders)


def array_escape(v: Any) -> str:
    """
    Escapes vulnerable str data for nested types as in arrays or hstore.

    To meet the nested rules above, the data must already be escaped from
    text_escape, e.g. \ --> text_escape --> \\ --> array_escape --> \\\\.
    """
    if not isinstance(v, str):
        v = str(v)
    return '"' + v.replace('\\\\', '\\\\\\\\').replace('"', '\\\\"') + '"'


# TODO: move the interface description into proper place...
"""
Encoder Interface

Field value encoding for postgres' TEXT format happens in 2 main steps:

- string stage:
    A string representation of a data row (e.g. for one model instance)
    is constructed by f-string formatting from return values of the field encoders.
    A byte intensive encoder may place a ``LAZY_PLACEHOLDER`` in the row string
    to indicate later data injection during the byte stage.
    Once the row is finished, it gets encoded to bytes by the selected encoding.

- byte stage:
    The bytes are written in 64k chunks to a file object digested by
    psycopg2's copy_from function, either directly or threaded for bigger payloads.
    If an encoder marked its data as lazy, the encoders lazy part gets called
    and writes its data directly into the file object.

Within the default encoders the lazy encoding is only used for binary fields to
avoid costly forth and back unicode to bytes encoding of potentially big data.


Field Encoder

A field encoder is responsible for conversion of a python type into a string format,
that Postgres understands and does not collide with the TEXT format.
The basic encoder interface is

def some_encoder(value: Any, fname: str, lazy: List[Any]) -> ReturnType: ...
some_encoder.array_escape: bool = ...

``value`` denotes the incoming field value as currently set on the model instance for
the field ``fname``. ``lazy`` is a context helper object for lazy encoders
(see Lazy Encoder below).

The return type should be a string in postgres' TEXT format. For proper escaping 
the helper ``text_escape`` can be used. For None/nullish values ``NULL`` is predefined.
It is also possible to return a python type directly, if it is known to translate
correctly into the TEXT format from ``__str__`` output (some default encoders
use it to speed up processing).

The ``array_escape`` flag is needed to indicate, whether additional escaping for
array handling is needed (see ``array_escape`` function). Typically this is
always the case for any `text_escape`'d values. Beside those, the array syntax has
further collisions for characters like comma or curly braces.

For better typings support there is a decorator ``encoder``, which can be applied
to the encoding function like this:

@encoder
def some_encoder(value: Any, fname: str, lazy: List[Any]) -> ReturnType: ...

The decorator sets the ``array_escape`` flag to ``True`` be default.


Lazy Encoder

Lazy encoding can be achieved by placing returning a ``LAZY_PLACEHOLDER`` and
appending a tuple like ``(callback, value)`` to ``lazy``. The callback is meant
to write bytes directly to the file object and has the following interface:

def lazy_callback(file: BinaryIO, value: Any) -> None:


Encoder Registration

On startup django's standard field types are globally mapped to default encoders.
With ``register_fieldclass`` the encoders can be changed, or encoders be registered
for a custom field type.

Furthermore encoders can be overridden for individual `copy_update` calls with
``field_encoders``.


Array Support

The default encoder implementations do not hardcode array support, but use
the factory function ``array_factory`` instead. The factory should be generic enough
to be used with custom encoders as well. Currently the factory has to re-run for every
arrayfield. Furthermore the empty and balance checks are not further optimised yet.
Thus arrayfields have some performance penalty currently.
"""

def encoder(func: EncoderProto) -> FieldEncoder[EncoderProto]:
    """
    Decoration helper for encoders to place the needed
    array_escape attribute (default is True).
    """
    f = cast(FieldEncoder[EncoderProto], func)
    f.array_escape = True
    return f


@encoder
def Int(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``int``, raise for any other."""
    if isinstance(v, int):
        return v
    raise TypeError(f'expected type {int} for field "{fname}", got {type(v)}')
Int.array_escape = False


@encoder
def IntOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Int``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, int):
        return v
    raise TypeError(f'expected type {int} or None for field "{fname}", got {type(v)}')
IntOrNone.array_escape = False


def _lazy_binary(f: BinaryIO, v: Union[memoryview, bytes]) -> None:
    length = len(v)
    if length <= 65536:
        f.write(b2a_hex(v))
    else:
        byte_pos = 0
        while (byte_pos < length):
            f.write(b2a_hex(v[byte_pos:byte_pos+65536]))
            byte_pos += 65536


@encoder
def Binary(v: Any, fname: str, lazy: List[Any]):
    """
    Test and pass along ``(memoryview, bytes)`` types, raise for any other.

    Binary data is transmitted in Postgres' HEX format, thus a single byte
    creates 2 hex digits in the transport representation.

    If bytelength is >4096, the encoding is post-poned to the byte stage
    to avoid unicode forth and back conversion of hex digits.
    """
    if isinstance(v, (memoryview, bytes)):
        if len(v) > 4096:
            lazy.append((_lazy_binary, v))
            return '\\\\x' + LAZY_PLACEHOLDER
        return '\\\\x' + v.hex()
    raise TypeError(f'expected types {memoryview} or {bytes} for field "{fname}", got {type(v)}')


@encoder
def BinaryOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Binary``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, (memoryview, bytes)):
        if len(v) > 4096:
            lazy.append((_lazy_binary, v))
            return '\\\\x' + LAZY_PLACEHOLDER
        return '\\\\x' + v.hex()
    raise TypeError(f'expected types {memoryview}, {bytes} or None for field "{fname}", got {type(v)}')


@encoder
def Boolean(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``bool``, raise for any other."""
    if isinstance(v, bool):
        return v
    raise TypeError(f'expected type {bool} for field "{fname}", got {type(v)}')
Boolean.array_escape = False


@encoder
def BooleanOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Boolean``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, bool):
        return v
    raise TypeError(f'expected type {bool} or None for field "{fname}", got {type(v)}')
BooleanOrNone.array_escape = False


@encoder
def Date(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``datetime.date``, raise for any other."""
    if isinstance(v, date):
        return v
    raise TypeError(f'expected type {date} for field "{fname}", got {type(v)}')
Date.array_escape = False


@encoder
def DateOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Date``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, date):
        return v
    raise TypeError(f'expected type {date} or None for field "{fname}", got {type(v)}')
DateOrNone.array_escape = False


@encoder
def Datetime(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``datetime``, raise for any other."""
    if isinstance(v, datetime):
        return v
    raise TypeError(f'expected type {datetime} for field "{fname}", got {type(v)}')


@encoder
def DatetimeOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Datetime``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, datetime):
        return v
    raise TypeError(f'expected type {datetime} or None for field "{fname}", got {type(v)}')


@encoder
def Numeric(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``Decimal``, raise for any other."""
    if isinstance(v, Decimal):
        return v
    raise TypeError(f'expected type {Decimal} for field "{fname}", got {type(v)}')
Numeric.array_escape = False


@encoder
def NumericOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Numeric``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, Decimal):
        return v
    raise TypeError(f'expected type {Decimal} or None for field "{fname}", got {type(v)}')
NumericOrNone.array_escape = False


@encoder
def Duration(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``timedelta``, raise for any other."""
    if isinstance(v, timedelta):
        return v
    raise TypeError(f'expected type {timedelta} for field "{fname}", got {type(v)}')


@encoder
def DurationOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Duration``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, timedelta):
        return v
    raise TypeError(f'expected type {timedelta} or None for field "{fname}", got {type(v)}')


@encoder
def Float(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``float`` or ``int``, raise for any other."""
    if isinstance(v, (float, int)):
        return v
    raise TypeError(f'expected types {float} or {int} for field "{fname}", got {type(v)}')
Float.array_escape = False


@encoder
def FloatOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Float``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, (float, int)):
        return v
    raise TypeError(f'expected types {float}, {int} or None for field "{fname}", got {type(v)}')
FloatOrNone.array_escape = False


# TODO: test and document Json vs. JsonOrNone behavior (sql null vs. json null)
# TODO: document alternative json field encoder with orjson?
@encoder
def Json(v: Any, fname: str, lazy: List[Any]):
    """
    Default JSON encoder using ``json.dumps``.

    This version encodes ``None`` as json null value.
    """
    return text_escape(dumps(v))


@encoder
def JsonOrNone(v: Any, fname: str, lazy: List[Any]):
    """
    Default JSON encoder using ``json.dumps``.

    This version encodes ``None`` as sql null value.
    """
    if v is None:
        return NULL
    return text_escape(dumps(v))


@encoder
def Text(v: Any, fname: str, lazy: List[Any]):
    """
    Test and encode ``str``, raise for any other.
    
    The encoder escapes characters as denoted in the postgres documentation
    for the TEXT format of COPY FROM.
    """
    if isinstance(v, str):
        return text_escape(v)
    raise TypeError(f'expected type {str} for field "{fname}", got {type(v)}')


@encoder
def TextOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Text``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, str):
        return text_escape(v)
    raise TypeError(f'expected type {str} or None for field "{fname}", got {type(v)}')


@encoder
def Time(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``datetime.time``, raise for any other."""
    if isinstance(v, dt_time):
        return v
    raise TypeError(f'expected type {dt_time} for field "{fname}", got {type(v)}')
Time.array_escape = False


@encoder
def TimeOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Time``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, dt_time):
        return v
    raise TypeError(f'expected type {dt_time} or None for field "{fname}", got {type(v)}')
TimeOrNone.array_escape = False


@encoder
def Uuid(v: Any, fname: str, lazy: List[Any]):
    """Test and pass along ``UUID``, raise for any other."""
    if isinstance(v, UUID):
        return v
    raise TypeError(f'expected type {UUID} for field "{fname}", got {type(v)}')
Uuid.array_escape = False


@encoder
def UuidOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Uuid``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, UUID):
        return v
    raise TypeError(f'expected type {UUID} or None for field "{fname}", got {type(v)}')
UuidOrNone.array_escape = False


@encoder
def HStore(v: Any, fname: str, lazy: List[Any]):
    """
    HStore field encoder. Expects a ``dict`` as input type
    with str keys and str|None values. Any other types will raise.

    The generated TEXT format representation is always quoted
    in the form: ``"key"=>"value with \\"double quotes\\""``.
    """
    if isinstance(v, dict):
        parts = []
        for k, v in v.items():
            if not isinstance(k, str):
                raise TypeError(f'expected type {str} for keys of field "{fname}"')
            if v is not None and not isinstance(v, str):
                raise TypeError(f'expected type {str} or None for values of field "{fname}"')
            parts.append(
                f'{array_escape(text_escape(k))}=>'
                f'{SQL_NULL if v is None else array_escape(text_escape(v))}'
            )
        return ','.join(parts)
    raise TypeError(f'expected type {dict} for field "{fname}", got {type(v)}')


@encoder
def HStoreOrNone(v: Any, fname: str, lazy: List[Any]):
    """Same as ``Hstore``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, dict):
        parts = []
        for k, v in v.items():
            if not isinstance(k, str):
                raise TypeError(f'expected type {str} for keys of field "{fname}"')
            if v is not None and not isinstance(v, str):
                raise TypeError(f'expected type {str} or None for values of field "{fname}"')
            parts.append(
                f'{array_escape(text_escape(k))}=>'
                f'{SQL_NULL if v is None else array_escape(text_escape(v))}'
            )
        return ','.join(parts)
    raise TypeError(f'expected type {dict} or None for field "{fname}", got {type(v)}')


def range_factory(basetype, text_safe) -> Tuple[FieldEncoder, FieldEncoder]:
    """
    Factory for range type encoders.

    The generated encoders always expect a psycopg2 ``Range`` type.
    Additionally the encoders test, that the ``lower`` and ``upper`` values
    set on the range object are of type ``basetype``.

    ``text_safe`` indicates, whether the values convert safely into postgres'
    TEXT format. This is the case for all range types defined from django,
    but might not be true anymore for hand-crafted range types.

    ``array_escape`` is always true for range types to avoid ambiguity
    with array commas.

    Returns a tuple of (Range, RangeOrNone) encoders.
    """
    @encoder
    def encode_range(v: Any, fname: str, lazy: List[Any]):
        if isinstance(v, Range) and isinstance(v.lower, basetype) and isinstance(v.upper, basetype):
            return v if text_safe else text_escape(str(v))
        raise TypeError(f'expected type {basetype} for field "{fname}", got {type(v)}')

    @encoder
    def encode_range_none(v: Any, fname: str, lazy: List[Any]):
        if v is None:
            return NULL
        if isinstance(v, Range) and isinstance(v.lower, basetype) and isinstance(v.upper, basetype):
            return v if text_safe else text_escape(str(v))
        raise TypeError(f'expected type {basetype} or None for field "{fname}", got {type(v)}')

    return encode_range, encode_range_none


# Edge cases with arrays:
# -   empty top level array works for both
#     select ARRAY[]::integer[];                  -->     {}
#     select '{}'::integer[];                     -->     {}
#
# -   nested empty sub arrays
#     select ARRAY[ARRAY[ARRAY[]]]::integer[];    -->     {}
#     select '{{{}}}'::integer[];                 -->     malformed array literal: "{{{}}}"
#     --> no direct aquivalent in text notation
#
# -   complicated mixture with null
#     select ARRAY[null,ARRAY[ARRAY[],null]]::integer[];      -->     {}
#     no direct aquivalent in text notation
#
# Is the ARRAY notation broken in postgres?
#
# Observations:
# - if all values are nullish + at least one empty sub array, enclosing array is set to empty (losing all inner info?)
#   --> cascades up to top level leaving an empty array {} with no dimension or inner info at all
# - if all values are nullish and there is no empty sub array, the values manifest as null with dimension set
# - ARRAY[] raises for unbalanced multidimension arrays, TEXT format returns nonsense syntax error
#
# The following 2 functions is_empty_array and is_balanced try to restore some of the ARRAY[] behavior.
# This happens to a rather high price of 2 additional deep scan of array values.
# A better implementation prolly could do that in one pass combined.


def is_empty_array(v: Any) -> bool:
    """
    Special handling of nullish array values, that reduce to single {}.

    This is purely taken from ARRAY[] behavioral observations,
    thus might not cover all edge cases yet.
    """
    if v == []:
        return True
    if all((is_empty_array(e) if isinstance(e, (list, tuple)) else e is None) for e in v):
        if all(e is None for e in v):
            return False
        return True
    return False


def _balanced(v: Any, depth: int, dim: int = 0) -> Union[Tuple, None]:
    if not isinstance(v, (list, tuple)) or dim>=depth:
        return None
    return tuple(_balanced(e, depth, dim+1) for e in v)

def is_balanced(v: Any, depth: int) -> bool:
    """
    Check if array value is balanced over multiple dimensions.
    """
    return len(set(_balanced(v, depth) or [])) < 2


def array_factory(encoder, depth=1, null=False) -> FieldEncoder:
    """
    Factory for array value encoder.

    The returned encoder tries to mimick ArrayField.get_db_prep_value + ARRAY[] conditions
    as close as possible:
    - allow array descent up to ``depth`` (get_db_prep_value restriction, postgres doesn't care)
    - final values may occur at any level up to depth
    - null setting respected at top level, always allowed in sub arrays
    - empty_array reduction (ARRAY[] behavior, see is_empty_array)
    - explicit balance chck in python (normally done in ARRAY, but not available in TEXT format)
    - array descent on types list and tuple

    ``encoder`` denotes the final type encoder to be applied.
    ``depth`` is the max array dimensions, the encoder will try to descend into subarrays.
    ``null`` denotes whether the encoder should allow None at top level.
    """
    def _encode_array(v: Any, fname: str, lazy: List[Any], dim: int = 0) -> str:
        if not dim:
            # handle top level separately, as it differs for certain aspects:
            # - null respected (postgres always allows null in nested arrays)
            # - need to check whether value reduces to empty array {}
            # - balance check
            if v is None:
                if null:
                    return NULL
                raise TypeError(f'expected type {list} or {tuple} for field "{fname}", got None')
            if isinstance(v, (list, tuple)):
                # test for empty reduction
                if is_empty_array(v):
                    return '{}'
                # other than bulk_update we have to do the balance check in python to get
                # a meaningful error, as postgres throws only an unspecific syntax error
                if depth > 1 and not is_balanced(v, depth):
                    raise ValueError(f'multidimensional arrays must be balanced, got {v}')
                return f'{{{",".join(encode_array(e, fname, lazy, dim+1) for e in v)}}}'
            raise TypeError(f'expected type {list} or {tuple} for field "{fname}", got {type(v)}')
        if v is None:
            return SQL_NULL
        if isinstance(v, (list, tuple)) and dim < depth:
            # multi dim arrays descent
            return f'{{{",".join(encode_array(e, fname, lazy, dim+1) for e in v)}}}'
        # fall-though to value encoding:
        # - any non list|tuple type
        # - always at bottom (dim==depth)
        final = str(encoder(v, fname, lazy))
        return array_escape(final) if encoder.array_escape else final
    encode_array = cast(FieldEncoder[Any], _encode_array)
    encode_array.array_escape = False
    return encode_array


# global field type -> encoder mapping
ENCODERS: Dict[Type[models.Field], Tuple[FieldEncoder, FieldEncoder]] = {
    models.AutoField: (Int, IntOrNone),
    models.BigAutoField: (Int, IntOrNone),
    models.BigIntegerField: (Int, IntOrNone),
    models.BinaryField: (Binary, BinaryOrNone),
    models.BooleanField: (Boolean, BooleanOrNone),
    models.CharField: (Text, TextOrNone),
    models.DateField: (Date, DateOrNone),
    models.DateTimeField: (Datetime, DatetimeOrNone),
    models.DecimalField: (Numeric, NumericOrNone),
    models.DurationField: (Duration, DurationOrNone),
    models.EmailField: (Text, TextOrNone),
    #models.FileField: (AsNotImpl, AsNotImpl), # should we disallow this? any workaround possible?
    #models.FilePathField: (AsNotImpl, AsNotImpl), # how to go about this one?
    models.FloatField: (Float, FloatOrNone),
    models.GenericIPAddressField: (Text, TextOrNone),
    #models.ImageField: (AsNotImpl, AsNotImpl), # same as FileField?
    models.IntegerField: (Int, IntOrNone),
    models.JSONField: (Json, JsonOrNone),
    models.PositiveBigIntegerField: (Int, IntOrNone),
    models.PositiveIntegerField: (Int, IntOrNone),
    models.PositiveSmallIntegerField: (Int, IntOrNone),
    models.SlugField: (Text, TextOrNone),
    models.SmallAutoField: (Int, IntOrNone),
    models.SmallIntegerField: (Int, IntOrNone),
    models.TextField: (Text, TextOrNone),
    models.TimeField: (Time, TimeOrNone),
    models.URLField: (Text, TextOrNone),
    models.UUIDField: (Uuid, UuidOrNone),
    # postgres specific fields
    HStoreField: (HStore, HStoreOrNone),
    IntegerRangeField: range_factory(int, True),
    BigIntegerRangeField: range_factory(int, True),
    DecimalRangeField: range_factory(Decimal, True),
    DateTimeRangeField: range_factory(datetime, True),
    DateRangeField: range_factory(date, True),
    # ArrayField: handled by array_factory(field) in get_encoder
}


def register_fieldclass(
    field_cls: Type[models.Field],
    encoder: FieldEncoder,
    encoder_none: Optional[FieldEncoder] = None
) -> None:
    """
    Register a fieldclass globally with value encoders.

    ``encoder`` will be used for fields constructed with ``null=False``,
    ``encoder_none`` for fields with ``null=True``.

    If only one encoder is provided, it will be used for both field settings.
    In that case make sure, that the encoder correctly translates None values.
    """
    ENCODERS[field_cls] = (encoder, encoder_none or encoder)


def get_encoder(field: models.Field, null: Optional[bool] = None) -> FieldEncoder:
    """Get registered encoder for field."""
    if null is None:
        null = field.null
    if isinstance(field, RelatedField):
        return get_encoder(field.target_field, null)
    if isinstance(field, ArrayField):
        # TODO: cache array encoder for later calls?
        base = field.base_field
        depth = 1
        while isinstance(base, ArrayField):
            base = base.base_field
            depth += 1
        return array_factory(get_encoder(base), depth, null)
    for cls in type(field).__mro__:
        enc = ENCODERS.get(cls)
        if enc:
            return enc[null]
    raise NotImplementedError(f'no suitable encoder found for field {field}')


def write_lazy(f: BinaryIO, data: bytearray, stack: List[Any]) -> None:
    """Execute lazy field encoders."""
    m = memoryview(data)
    idx = 0
    for writer, byte_object in stack:
        old = idx
        idx = data.index(LAZY_PLACEHOLDER_BYTE, idx)
        f.write(m[old:idx])
        writer(f, byte_object)
        idx += 1
    f.write(m[idx:])


def threaded_copy(
    c: CursorWrapper,
    fr: BinaryIO,
    tname: str,
    columns: Tuple[str]
) -> None:
    c.copy_from(fr, tname, size=65536, columns=columns)


def copy_from(
    c: CursorWrapper,
    tname: str,
    data: Sequence[models.Model],
    fnames: Tuple[str],
    columns: Tuple[str],
    get: attrgetter,
    encs: List[Any],
    encoding: str
) -> None:
    """
    Optimized call of cursor.copy_from with threading for bigger change data.
    """
    use_thread = False
    payload = bytearray()
    lazy: List[Any] = []
    for o in data:
        payload += '\t'.join([
            f'{enc(el, fname, lazy)}'
            for enc, el, fname in zip(encs, get(o), fnames)
        ]).encode(encoding)
        payload += b'\n'
        if len(payload) > 65535:
            # if we exceed 64k, switch to threaded chunkwise processing
            if not use_thread:
                r, w = os.pipe()
                fr = os.fdopen(r, 'rb')
                fw = os.fdopen(w, 'wb')
                t = Thread(
                    target=threaded_copy,
                    args=[c.connection.cursor(), fr, tname, columns]
                )
                t.start()
                use_thread = True
            if lazy:
                write_lazy(fw, payload, lazy)
                lazy.clear()
                payload = bytearray()
            else:
                length = len(payload)
                m = memoryview(payload)
                pos = 0
                while length - pos > 65535:
                    # write all full 64k chunks (in case some line payload went overboard)
                    fw.write(m[pos:pos+65536])
                    pos += 65536
                # carry remaining data forward
                payload = bytearray(m[pos:])
    if use_thread:
        if payload:
            if lazy:
                write_lazy(fw, payload, lazy)
            else:
                fw.write(payload)
        # closing order important:
        # - close write end -> threaded copy_from drains pipe and finishes
        # - wait for thread termination
        # - close read end
        fw.close()
        t.join()
        fr.close()
    elif payload:
        if lazy:
            f = BytesIO()
            write_lazy(f, payload, lazy)
            f.seek(0)
        else:
            f = BytesIO(payload)
        c.copy_from(f, tname, size=65536, columns=columns)
        f.close()


def create_columns(column_def: Tuple[str, str]) -> str:
    """
    Prepare columns for table create as follows:
    - types copied from target table
    - no indexes or constraints (no serial, no unique, no primary key etc.)
    """
    return (','.join(f'{k} {v}' for k, v in column_def)
        .replace('bigserial', 'bigint')
        .replace('smallserial', 'smallint')
        .replace('serial', 'integer')
    )


def update_sql(
    tname: str,
    temp_table: str,
    pkname: str,
    copy_fields: List[models.Field]
) -> str:
    cols = ','.join(f'"{f.column}"="{temp_table}"."{f.column}"' for f in copy_fields)
    where = f'"{tname}"."{pkname}"="{temp_table}"."{pkname}"'
    return f'UPDATE "{tname}" SET {cols} FROM "{temp_table}" WHERE {where}'


def copy_update(
    qs: models.QuerySet,
    objs: Sequence[models.Model],
    fieldnames: Iterable[str],
    field_encoders: Optional[Dict[str, Any]] = None,
    encoding: Optional[str] = None
) -> int:
    qs._for_write = True
    conn = connections[qs.db]
    model = qs.model

    # filter all non model local fields --> still handled by bulk_update
    non_local_fieldnames = []
    local_fieldnames = []
    for fieldname in fieldnames:
        if model._meta.get_field(fieldname) not in model._meta.local_fields:
            non_local_fieldnames.append(fieldname)
        else:
            local_fieldnames.append(fieldname)

    if not local_fieldnames:
        return qs.bulk_update(objs, non_local_fieldnames)

    pk_field = model._meta.pk
    fields = [model._meta.get_field(fname) for fname in local_fieldnames]
    all_fields = [pk_field] + fields
    attnames, colnames, column_def = zip(*[
        (f.attname, f.column, (f.column, f.db_type(conn)))
            for f in all_fields])
    encs = ([field_encoders.get(f.attname, get_encoder(f)) for f in all_fields]
        if field_encoders else [get_encoder(f) for f in all_fields])
    get = attrgetter(*attnames)
    rows_updated = 0
    with transaction.atomic(using=conn.alias, savepoint=False), conn.cursor() as c:
        temp = f'temp_cu_{model._meta.db_table}'
        c.execute(f'DROP TABLE IF EXISTS "{temp}"')
        c.execute(f'CREATE TEMPORARY TABLE "{temp}" ({create_columns(column_def)})')
        copy_from(c, temp, objs, attnames, colnames, get, encs,
            encoding or CONNECTION_ENCODINGS[c.connection.encoding])
        c.execute(f'ANALYZE "{temp}" ({pk_field.column})')
        c.execute(update_sql(model._meta.db_table, temp, pk_field.column, fields))
        rows_updated = c.rowcount
        c.execute(f'DROP TABLE "{temp}"')

        # handle remaining non local fields (done by bulk_update for now)
        if non_local_fieldnames:
            _rows_updated = qs.bulk_update(objs, non_local_fieldnames)
            rows_updated = max(rows_updated, _rows_updated or 0)

    return rows_updated
