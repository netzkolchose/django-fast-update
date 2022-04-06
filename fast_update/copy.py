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


NULL = '\\N'
BYTE_PLACEHOLDER = '\x00'
BYTE_PLACEHOLDER_BYTE = b'\x00'

# TODO: copy encoders and array impl from playground
# TODO: tons of tests...


def AsNone(v, lazy):
    """Treat field value as ``None`` converted to NULL."""
    return NULL


def AsIs(v, lazy):
    """
    Field value passed along unchecked.

    Can be used for a performance gain, if all provided field values
    are known to correctly translate into Postgres' COPY TEXT format
    by python's string formatting. That is sometimes the case for values,
    where type checking/narrowing happened in an earlier step.
    Python types known to work that way are: int, float, ...
    Nullish field values may use ``AsIsOrNone`` instead.
    When used for string values, make sure that the strings never contain
    characters, that need explicit escaping in the TEXT format.
    """
    # FIXME: move description above to top level docs
    return v


def AsIsOrNone(v, lazy):
    """Same as ``AsIs``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    return v


def Int(v, lazy):
    """Test and pass along ``int``, raise for any other."""
    if isinstance(v, int):
        return v
    raise TypeError('expected int type')


def IntOrNone(v, lazy):
    """Same as ``Int``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, int):
        return v
    raise TypeError('expected int or NoneType')


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
    creates 2 hex digits in the 
    If bytelength is >4096, the encoding is post-poned to the byte stage
    to avoid unicode forth and back conversion of hex digits. While this greatly
    lowers the memory usage and runtime for bigger binary data, it is still 
    """
    if isinstance(v, (memoryview, bytes)):
        if len(v) > 4096:
            lazy.append((_lazy_binary, v))
            return '\\\\x' + BYTE_PLACEHOLDER
        return '\\\\x' + v.hex()
    raise TypeError('expected memoryview or bytes type')


def BinaryOrNone(v, lazy):
    """Same as ``Binary``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, (memoryview, bytes)):
        if len(v) > 4096:
            lazy.append((_lazy_binary, v))
            return '\\\\x' + BYTE_PLACEHOLDER
        return '\\\\x' + v.hex()
    raise TypeError('expected memoryview, bytes or NoneType')


def Boolean(v, lazy):
    if isinstance(v, bool):
        return v
    raise TypeError('expected bool type')


def BooleanOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, bool):
        return v
    raise TypeError('expected bool type or NoneType')


def Date(v, lazy):
    if isinstance(v, date):
        return v
    raise TypeError('expected date type')


def DateOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, date):
        return v
    raise TypeError('expected date type or NoneType')


def Datetime(v, lazy):
    if isinstance(v, datetime):
        return v
    raise TypeError('expected datetime type')


def DatetimeOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, datetime):
        return v
    raise TypeError('expected datetime type or NoneType')


def Numeric(v, lazy):
    if isinstance(v, Decimal):
        return v
    raise TypeError('expected Decimal type')


def NumericOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, Decimal):
        return v
    raise TypeError('expected Decimal type or NoneType')


def Duration(v, lazy):
    if isinstance(v, timedelta):
        return v
    raise TypeError('expected timedelta type')


def DurationOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, timedelta):
        return v
    raise TypeError('expected timedelta type or NoneType')


def Float(v, lazy):
    if isinstance(v, (float, int)):
        return v
    raise TypeError('expected float or int type')


def FloatOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, (float, int)):
        return v
    raise TypeError('expected float, int or NoneType')


def Json(v, lazy):
    return Text(dumps(v), lazy)


def JsonOrNone(v, lazy):
    if v is None:
        return NULL
    return Text(dumps(v), lazy)


def Text(v, lazy):
    """
    Test and encode ``str``, raise for any other.
    
    The encoder escapes characters as denoted in the postgres documentation
    for the TEXT format of COPY FROM.
    """
    if isinstance(v, str):
        return (v.replace('\\', '\\\\')
            .replace('\b', '\\b').replace('\f', '\\f').replace('\n', '\\n')
            .replace('\r', '\\r').replace('\t', '\\t').replace('\v', '\\v'))
    raise TypeError('expected str type')


def TextOrNone(v, lazy):
    """Same as ``Text``, additionally handling ``None`` as NULL."""
    if v is None:
        return NULL
    if isinstance(v, str):
        return (v.replace('\\', '\\\\')
            .replace('\b', '\\b').replace('\f', '\\f').replace('\n', '\\n')
            .replace('\r', '\\r').replace('\t', '\\t').replace('\v', '\\v'))
    raise TypeError('expected str or NoneType')


def Time(v, lazy):
    if isinstance(v, dt_time):
        return v
    raise TypeError('expected datetime.time type')


def TimeOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, dt_time):
        return v
    raise TypeError('expected datetime.time type or NoneType')


def Uuid(v, lazy):
    if isinstance(v, UUID):
        return v
    raise TypeError('expected UUID type')


def UuidOrNone(v, lazy):
    if v is None:
        return NULL
    if isinstance(v, UUID):
        return v
    raise TypeError('expected UUID type or NoneType')


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
    #ArrayField, HStore, Range ...
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
        idx = data.index(BYTE_PLACEHOLDER_BYTE, idx)
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


def prepare_create_columns(column_def):
    """
    Prepare columns for table create as follows:
    - types copied from target table
    - no indexes or constraints (no serial, no unique, no primary key etc.)
    """
    # FIXME: Should we enforce pk NOT NULL?
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
    transport_encoding: Optional[str] = 'utf-8',
    field_encoders: Optional[Dict[str, Any]] = None
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
        c.execute(f'CREATE TEMPORARY TABLE "{temp}" ({prepare_create_columns(column_def)})')
        copy_from(c, temp, objs, colnames, get, encs, transport_encoding)
        c.execute(f'ANALYZE "{temp}" ({pk_field.column})')
        c.execute(update_sql(model._meta.db_table, temp, pk_field.column, fields))
        rows_updated = c.rowcount
        c.execute(f'DROP TABLE "{temp}"')

        # handle remaining non local fields (done by bulk_update for now)
        if non_local_fieldnames:
            _rows_updated = qs.bulk_update(objs, non_local_fieldnames)
            rows_updated = max(rows_updated, _rows_updated or 0)

    return rows_updated
