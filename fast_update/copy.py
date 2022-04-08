import os
from threading import Thread
from io import BytesIO
from binascii import b2a_hex
from operator import attrgetter
from django.db import connections, transaction, models
from typing import Any, Dict, Optional, Sequence
from decimal import Decimal as Decimal
from datetime import date, datetime, timedelta, time as dt_time
from json import dumps
from uuid import UUID
from django.contrib.postgres.fields import (HStoreField, ArrayField, IntegerRangeField,
    BigIntegerRangeField, DecimalRangeField, DateTimeRangeField, DateRangeField)
from psycopg2.extras import Range


# TODO: typings, docs cleanup
# TODO: import guard in query.py


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
# lazy placeholder: NUL - not allowed in postgres data (cannot slip through)
LAZY_PLACEHOLDER = '\x00'
LAZY_PLACEHOLDER_BYTE = b'\x00'


def text_escape(v):
    """
    Escape str-like data for postgres' TEXT format.
    """
    return (v.replace('\\', '\\\\')
        .replace('\b', '\\b').replace('\f', '\\f').replace('\n', '\\n')
        .replace('\r', '\\r').replace('\t', '\\t').replace('\v', '\\v'))


def Int(v, lazy):
    """Test and pass along ``int``, raise for any other."""
    if isinstance(v, int):
        return v
    raise TypeError(f'expected type {int}')
Int.array_escape = False


def IntOrNone(v, lazy):
    """Same as ``Int``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, int):
        return v
    raise TypeError(f'expected type {int} or None')
IntOrNone.array_escape = False


def _lazy_binary(f, v):
    length = len(v)
    if length <= 65536:
        f.write(b2a_hex(v))
    else:
        byte_pos = 0
        while (byte_pos < length):
            f.write(b2a_hex(v[byte_pos:byte_pos+65536]))
            byte_pos += 65536


def Binary(v, lazy):
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
    raise TypeError(f'expected types {memoryview} or {bytes}')
Binary.array_escape = True


def BinaryOrNone(v, lazy):
    """Same as ``Binary``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, (memoryview, bytes)):
        if len(v) > 4096:
            lazy.append((_lazy_binary, v))
            return '\\\\x' + LAZY_PLACEHOLDER
        return '\\\\x' + v.hex()
    raise TypeError(f'expected types {memoryview}, {bytes} or None')
BinaryOrNone.array_escape = True


def Boolean(v, lazy):
    """Test and pass along ``bool``, raise for any other."""
    if isinstance(v, bool):
        return v
    raise TypeError(f'expected type {bool}')
Boolean.array_escape = False


def BooleanOrNone(v, lazy):
    """Same as ``Boolean``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, bool):
        return v
    raise TypeError(f'expected type {bool} or None')
BooleanOrNone.array_escape = False


def Date(v, lazy):
    """Test and pass along ``datetime.date``, raise for any other."""
    if isinstance(v, date):
        return v
    raise TypeError(f'expected type {date}')
Date.array_escape = False


def DateOrNone(v, lazy):
    """Same as ``Date``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, date):
        return v
    raise TypeError(f'expected type {date} or None')
DateOrNone.array_escape = False


def Datetime(v, lazy):
    """Test and pass along ``datetime``, raise for any other."""
    if isinstance(v, datetime):
        return v
    raise TypeError(f'expected type {datetime}')
Datetime.array_escape = True


def DatetimeOrNone(v, lazy):
    """Same as ``Datetime``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, datetime):
        return v
    raise TypeError(f'expected type {datetime} or None')
DatetimeOrNone.array_escape = True


def Numeric(v, lazy):
    """Test and pass along ``Decimal``, raise for any other."""
    if isinstance(v, Decimal):
        return v
    raise TypeError(f'expected type {Decimal}')
Numeric.array_escape = False


def NumericOrNone(v, lazy):
    """Same as ``Numeric``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, Decimal):
        return v
    raise TypeError(f'expected type {Decimal} or None')
NumericOrNone.array_escape = False


def Duration(v, lazy):
    """Test and pass along ``timedelta``, raise for any other."""
    if isinstance(v, timedelta):
        return v
    raise TypeError(f'expected type {timedelta}')
Duration.array_escape = True


def DurationOrNone(v, lazy):
    """Same as ``Duration``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, timedelta):
        return v
    raise TypeError(f'expected type {timedelta} or None')
DurationOrNone.array_escape = True


def Float(v, lazy):
    """Test and pass along ``float`` or ``int``, raise for any other."""
    if isinstance(v, (float, int)):
        return v
    raise TypeError(f'expected types {float} or {int}')
Float.array_escape = False


def FloatOrNone(v, lazy):
    """Same as ``Float``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, (float, int)):
        return v
    raise TypeError(f'expected types {float}, {int} or None')
FloatOrNone.array_escape = False


# TODO: test and document Json vs. JsonOrNone behavior (sql null vs. json null)
def Json(v, lazy):
    """
    Default JSON encoder using ``json.dumps``.

    This version encodes ``None`` as json null value.
    """
    return text_escape(dumps(v))
Json.array_escape = True


def JsonOrNone(v, lazy):
    """
    Default JSON encoder using ``json.dumps``.

    This version encodes ``None`` as sql null value.
    """
    if v is None:
        return NULL
    return text_escape(dumps(v))
JsonOrNone.array_escape = True


def Text(v, lazy):
    """
    Test and encode ``str``, raise for any other.
    
    The encoder escapes characters as denoted in the postgres documentation
    for the TEXT format of COPY FROM.
    """
    if isinstance(v, str):
        return text_escape(v)
    raise TypeError(f'expected type {str}')
Text.array_escape = True


def TextOrNone(v, lazy):
    """Same as ``Text``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, str):
        return text_escape(v)
    raise TypeError(f'expected type {str} or None')
TextOrNone.array_escape = True


def Time(v, lazy):
    """Test and pass along ``datetime.time``, raise for any other."""
    if isinstance(v, dt_time):
        return v
    raise TypeError(f'expected type {dt_time}')
Time.array_escape = False


def TimeOrNone(v, lazy):
    """Same as ``Time``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, dt_time):
        return v
    raise TypeError(f'expected type {dt_time} or None')
TimeOrNone.array_escape = False


def Uuid(v, lazy):
    """Test and pass along ``UUID``, raise for any other."""
    if isinstance(v, UUID):
        return v
    raise TypeError(f'expected type {UUID}')
Uuid.array_escape = False


def UuidOrNone(v, lazy):
    """Same as ``Uuid``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, UUID):
        return v
    raise TypeError(f'expected type {UUID} or None')
UuidOrNone.array_escape = False


"""
Special handling of nested types

Nested types behave way different in COPY FROM TEXT format,
than on top level (kinda falling back on SQL syntax format).
From the tests this applies to values in arrays and hstore
(prolly applies to all custom composite types, not tested).

Rules for nested types:
- a backslash in nested strings needs 4x escaping, e.g. \ --> \\\\
- nested str values may need explicit quoting --> always quoted
- due to quoting, " needs \\" escape in nested strings
- null value is again sql NULL
"""
def quote(v):
    return '"' + v.replace('"', '\\\\"') + '"'

def text_escape_nested(v):
    """
    Escape nested str-like data for postgres' TEXT format.
    The nested variant is needed for array and hstore data
    (prolly for any custom composite types, untested).
    """
    return (v.replace('\\', '\\\\\\\\')
        .replace('\b', '\\b').replace('\f', '\\f').replace('\n', '\\n')
        .replace('\r', '\\r').replace('\t', '\\t').replace('\v', '\\v'))

SQL_NULL = 'NULL'

def array_escape(v):
    return '"' + v.replace('\\\\', '\\\\\\\\').replace('"', '\\\\"') + '"'


def HStore(v, lazy):
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
                raise TypeError(f'expected type {str} for keys')
            if v is not None and not isinstance(v, str):
                raise TypeError(f'expected type {str} or None for values')
            parts.append(
                f'{quote(text_escape_nested(k))}=>'
                f'{SQL_NULL if v is None else quote(text_escape_nested(v))}'
            )
        return ','.join(parts)
    raise TypeError(f'expected type {dict}')
HStore.array_escape = True


def HStoreOrNone(v, lazy):
    """Same as ``Hstore``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, dict):
        return HStore(v, lazy)
    raise TypeError(f'expected type {dict} or None')
HStoreOrNone.array_escape = True


def range_factory(_type):
    def wrap(v, lazy):
        if isinstance(v, Range) and all(map(lambda e: isinstance(e, _type), (v.lower, v.upper))):
            return v
        raise TypeError(f'expected type {_type}')
    wrap.array_escape = True
    def wrap_none(v, lazy):
        if v is None:
            return NULL
        if isinstance(v, Range) and all(map(lambda e: isinstance(e, _type), (v.lower, v.upper))):
            return v
        raise TypeError(f'expected type {_type} or None')
    wrap_none.array_escape = True
    return wrap, wrap_none


# TODO: cache encoder for later calls?
# NOTE: array descent only happens on lists (to allow tuples as json native)
# FIXME: should we respect null field setting in dim descent?
def array_factory(field):
    base = field.base_field
    while isinstance(base, ArrayField):
        base = base.base_field
    enc = get_encoder(base)
    def wrap(v, lazy, dim=0):
        if v is None:
            return SQL_NULL if dim else NULL
        if isinstance(v, (list,)):
            return f'{{{",".join(wrap(e, lazy, dim+1) for e in v)}}}'
        final = str(enc(v, lazy))
        return array_escape(final) if enc.array_escape else final
    return wrap


ENCODERS = {
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
    IntegerRangeField: range_factory(int),
    BigIntegerRangeField: range_factory(int),
    DecimalRangeField: range_factory(Decimal),
    DateTimeRangeField: range_factory(datetime),
    DateRangeField: range_factory(date),
    # ArrayField: handled by array_factory(field) in get_encoder
}


def register_fieldclass(field_cls, encoder, encoder_none=None):
    """
    Register a fieldclass globally with value encoders.

    ``encoder`` will be used for fields constructed with ``null=False``,
    ``encoder_none`` for fields with ``null=True``.

    If only one encoder is provided, it will be used for both field settings.
    In that case make sure, that the encoder correctly translates ``None``.
    """
    ENCODERS[field_cls] = (encoder, encoder_none or encoder)


def get_encoder(field):
    """Get registered encoder for field."""
    if field.is_relation:
        return get_encoder(field.target_field)
    if isinstance(field, ArrayField):
        return array_factory(field)
    for cls in type(field).__mro__:
        enc = ENCODERS.get(cls)
        if enc:
            return enc[field.null]
    raise NotImplementedError(f'no suitable encoder found for field {field}')


def write_lazy(f, data, stack):
    """Execute lazy value encoders."""
    m = memoryview(data)
    idx = 0
    for writer, byte_object in stack:
        old = idx
        idx = data.index(LAZY_PLACEHOLDER_BYTE, idx)
        f.write(m[old:idx])
        writer(f, byte_object)
        idx += 1
    f.write(m[idx:])


def threaded_copy(cur, fr, tname, columns):
    cur.copy_from(fr, tname, size=65536, columns=columns)


def copy_from(c, tname, data, columns, get, encs, encoding):
    use_thread = False
    payload = bytearray()
    lazy = []
    for o in data:
        payload += '\t'.join([f'{enc(el, lazy)}' for enc, el in zip(encs, get(o))]).encode(encoding)
        payload += b'\n'
        if len(payload) > 65535:
            # if we exceed 64k, switch to threaded chunkwise processing
            if not use_thread:
                r, w = os.pipe()
                fr = os.fdopen(r, 'rb')
                fw = os.fdopen(w, 'wb')
                t = Thread(target=threaded_copy, args=[c.connection.cursor(), fr, tname, columns])
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


def create_columns(column_def):
    """
    Prepare columns for table create as follows:
    - types copied from target table
    - no indexes or constraints (no serial, no unique, no primary key etc.)
    """
    return (",".join(f'{k} {v}' for k, v in column_def)
        .replace('bigserial', 'bigint')
        .replace('smallserial', 'smallint')
        .replace('serial', 'integer')
    )


def update_sql(tname, temp_table, pkname, copy_fields):
    cols = ','.join(f'"{f.column}"="{temp_table}"."{f.column}"' for f in copy_fields)
    where = f'"{tname}"."{pkname}"="{temp_table}"."{pkname}"'
    return f'UPDATE "{tname}" SET {cols} FROM "{temp_table}" WHERE {where}'


def copy_update(
    qs: models.QuerySet,
    objs: Sequence[models.Model],
    fieldnames: Sequence[str],
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
    attnames, colnames, encs, column_def = zip(*[
        (f.attname, f.column, get_encoder(f), (f.column, f.db_type(conn)))
            for f in all_fields])
    if field_encoders:
        for fname, encoder in field_encoders.items():
            if fname in attnames:
                encs[attnames.index(fname)] = encoder
    get = attrgetter(*attnames)
    rows_updated = 0
    with transaction.atomic(using=conn.alias, savepoint=False), conn.cursor() as c:
        temp = f'temp_cu_{model._meta.db_table}'
        c.execute(f'DROP TABLE IF EXISTS "{temp}"')
        c.execute(f'CREATE TEMPORARY TABLE "{temp}" ({create_columns(column_def)})')
        copy_from(c, temp, objs, colnames, get, encs,
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
