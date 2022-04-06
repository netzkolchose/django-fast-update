from weakref import WeakKeyDictionary
from django.db import transaction, models, connections
from django.db.utils import ProgrammingError
from django.db.models.functions import Cast
from django.db.models.expressions import Col
from operator import attrgetter
import logging

# typing imports
from django.db.models import Field
from typing import List, Optional, Sequence, Any, Union
from django.db.models.sql.compiler import SQLCompiler
from django.db.backends.utils import CursorWrapper
from django.db.backends.base.base import BaseDatabaseWrapper


logger = logging.getLogger(__name__)


# memorize fast_update vendor on connection object
SEEN_CONNECTIONS = WeakKeyDictionary()


def get_vendor(conn: BaseDatabaseWrapper) -> str:
    """
    Get vendor name for fast_update implementation, or empty string.

    Due to differences in mariadb/mysql8 we cannot rely only on
    django's connection.vendor differentiation, but have to
    distinguish between 'mysql' (recent mariadb) and 'mysql8'.
    Returns empty string for unknown/unsupported db backends,
    on which fast_update will fall back to bulk_update.
    """
    vendor = SEEN_CONNECTIONS.get(conn)
    if vendor is not None:
        return vendor

    if conn.vendor == 'postgresql':
        SEEN_CONNECTIONS[conn] = 'postgresql'
        return 'postgresql'

    if conn.vendor == 'sqlite':
        major, minor, _ = conn.Database.sqlite_version_info
        if (major == 3 and minor > 32) or major > 3:
            SEEN_CONNECTIONS[conn] = 'sqlite'
            return 'sqlite'
        else:  # pragma: no cover
            logger.warning('unsupported sqlite backend, fast_update will fall back to bulk_update')
            SEEN_CONNECTIONS[conn] = ''
            return ''

    if conn.vendor == 'mysql':
        try:
            with transaction.atomic(using=conn.alias), conn.cursor() as c:
                c.execute("SELECT foo.0 FROM (VALUES (0, 1), (1, 'zzz'),(2, 'yyy')) as foo")
            SEEN_CONNECTIONS[conn] = 'mysql'
            return 'mysql'
        except ProgrammingError:
            pass
        try:
            with transaction.atomic(using=conn.alias), conn.cursor() as c:
                c.execute("SELECT column_1 FROM (VALUES ROW(1, 'zzz'), ROW(2, 'yyy')) as foo")
            SEEN_CONNECTIONS[conn] = 'mysql8'
            return 'mysql8'
        except ProgrammingError:  # pragma: no cover
            logger.warning('unsupported mysql backend, fast_update will fall back to bulk_update')
            SEEN_CONNECTIONS[conn] = ''
            return ''

    logger.warning('unsupported db backend, fast_update will fall back to bulk_update')
    SEEN_CONNECTIONS[conn] = ''
    return ''


def pq_cast(tname: str, field: Field, compiler: SQLCompiler, connection: Any) -> str:
    """Column type cast for postgres."""
    # FIXME: compare to as_postgresql in v4
    return Cast(Col(tname, field), output_field=field).as_sql(compiler, connection)[0]


def as_postgresql(
    tname: str,
    pkname: str,
    fields: Sequence[Field],
    rows: List[str],
    count: int,
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> str:
    """
    Uses UPDATE FROM VALUES with column aliasing.

    Other than other supported backends postgres is very picky about data types
    in the SET clause, therefore we always place an explicit type cast.
    """
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'"{f.column}"={pq_cast(dname, f, compiler, connection)}' for f in fields)
    values = ','.join(rows)
    dcols = f'"{pkname}",' + ','.join(f'"{f.column}"' for f in fields)
    where = f'"{tname}"."{pkname}"="{dname}"."{pkname}"'
    return (
        f'UPDATE "{tname}" '
        f'SET {cols} FROM (VALUES {values}) AS "{dname}" ({dcols}) WHERE {where}'
    )


def as_sqlite(
    tname: str,
    pkname: str,
    fields: Sequence[Field],
    rows: List[str],
    count: int,
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> str:
    """
    sqlite >= 3.32 implements basic UPDATE FROM VALUES support following postgres' syntax.
    Other than postgres, sqlite does not allow column aliasing, it always uses a fixed
    naming scheme (column1, column2, ...).
    """
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'"{f.column}"="{dname}"."column{i + 2}"'
        for i, f in enumerate(fields))
    values = ','.join(rows)
    where = f'"{tname}"."{pkname}"="{dname}"."column1"'
    return f'UPDATE "{tname}" SET {cols} FROM (VALUES {values}) AS "{dname}" WHERE {where}'


def as_mysql(
    tname: str,
    pkname: str,
    fields: Sequence[Field],
    rows: List[str],
    count: int,
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> str:
    """
    For mariadb we use TVC, introduced in 10.3.3.
    (see https://mariadb.com/kb/en/table-value-constructors/)

    Mariadb's TVC support lacks several features like column aliasing,
    instead it pulls the column names from the first data row.
    To deal with that weird behavior, we prepend a row with increasing numbers
    as column names (0,1, ...). To avoid wrong pk matches against that
    fake data row during the update join, the values table gets applied
    with an offset by 1 select.
    """
    dname = 'd' if tname != 'd' else 'c'
    temp = 'temp1' if tname != 'temp1' else 'temp2'
    cols = ','.join(f'`{tname}`.`{f.column}`={dname}.{i+1}' for i, f in enumerate(fields))
    # mysql only: prepend placeholders for additional (0,1,2,...) row
    values = ','.join([f'({",".join(["%s"] * (len(fields) + 1))})'] + rows)
    where = f'`{tname}`.`{pkname}` = {dname}.0'
    return (
        f'UPDATE `{tname}`, '
        f'(SELECT * FROM (VALUES {values}) AS {temp} LIMIT {count} OFFSET 1) AS {dname} '
        f'SET {cols} WHERE {where}'
    )


def as_mysql8(
    tname: str,
    pkname: str,
    fields: Sequence[Field],
    rows: List[str],
    count: int,
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> str:
    """
    For MySQL we use the extended VALUES statement, introduced in MySQL 8.0.19.
    (see https://dev.mysql.com/doc/refman/8.0/en/values.html)

    It differs alot from mariadb's TVC, thus we have to handle it separately.
    """
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'`{f.column}`={dname}.column_{i+1}' for i, f in enumerate(fields))
    values = ','.join('ROW' + r for r in rows)
    on = f'`{tname}`.`{pkname}` = {dname}.column_0'
    return f'UPDATE `{tname}` INNER JOIN (VALUES {values}) AS {dname} ON {on} SET {cols}'


# TODO: Make is pluggable for other vendors? (also support check)
QUERY = {
    'sqlite': as_sqlite,
    'postgresql': as_postgresql,
    'mysql': as_mysql,
    'mysql8': as_mysql8
}


def update_from_values(
    c: CursorWrapper,
    vendor: str,
    tname: str,
    pk_field: Field,
    fields: List[Field],
    counter: int,
    data: List[Any],
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> int:
    """
    Generate vendor specific sql statement and execute it for given data.
    """
    # The following placeholder calc is quite cumbersome:
    # For fast processing we approach the data col-based (90° turned)
    # to save runtime for funcion pointer juggling for every single row
    # which is ~90% faster than a more direct row-based evaluation.
    # This is still alot slower than direct flat layouting, but not significant
    # anymore for the total runtime (<<1%, a row-based approach takes 3-7%).
    row_fields = [pk_field] + fields
    row_length = len(row_fields)
    default_placeholder = ['%s'] * counter
    col_placeholders = [
        ([
            f.get_placeholder(data[i], compiler, connection)
            for i in range(pos, len(data), row_length)
        ] if hasattr(f, 'get_placeholder') else default_placeholder)
        for pos, f in enumerate(row_fields)
    ]
    rows = [f'({",".join(row)})' for row in zip(*col_placeholders)]
    sql = QUERY[vendor](tname, pk_field.column, fields, rows, counter, compiler, connection)
    if vendor == 'mysql':
        # mysql only: prepend (0,1,2,...) as first row
        data = list(range(len(fields) + 1)) + data
    c.execute(sql, data)
    return c.rowcount


def fast_update(
    qs: models.QuerySet,
    objs: Sequence[models.Model],
    fieldnames: Sequence[str],
    batch_size: Union[int, None]
) -> int:
    qs._for_write = True
    conn = connections[qs.db]
    model = qs.model

    # fall back to bulk_update if we dont have a working fast_update impl
    vendor = get_vendor(conn)
    if not vendor:  # pragma: no cover
        return qs.bulk_update(objs, fieldnames, batch_size)

    # filter all non model local fields --> still handled by bulk_update
    non_local_fieldnames = []
    local_fieldnames = []
    for fieldname in fieldnames:
        if model._meta.get_field(fieldname) not in model._meta.local_fields:
            non_local_fieldnames.append(fieldname)
        else:
            local_fieldnames.append(fieldname)

    if not local_fieldnames:
        return qs.bulk_update(objs, non_local_fieldnames, batch_size)
    
    # prepare all needed arguments for update
    max_batch_size = conn.ops.bulk_batch_size(['pk'] + local_fieldnames, objs)
    batch_size = min(batch_size or 2 ** 31, max_batch_size)
    fields = [model._meta.get_field(f) for f in local_fieldnames]
    pk_field = model._meta.pk
    get = attrgetter(pk_field.attname, *(f.attname for f in fields))
    prep_save = [pk_field.get_db_prep_save] + [f.get_db_prep_save for f in fields]
    compiler = models.sql.UpdateQuery(model).get_compiler(conn.alias)
    
    rows_updated = 0
    with transaction.atomic(using=conn.alias, savepoint=False):
        # update data either batched or in one go
        with conn.cursor() as c:
            data = []
            counter = 0
            for o in objs:
                counter += 1
                data += [p(v, conn) for p, v in zip(prep_save, get(o))]
                if counter >= batch_size:
                    rows_updated += update_from_values(
                        c, vendor, model._meta.db_table, pk_field, fields,
                        counter, data, compiler, conn
                    )
                    data = []
                    counter = 0
            if data:
                rows_updated += update_from_values(
                    c, vendor, model._meta.db_table, pk_field, fields,
                    counter, data, compiler, conn
                )

        # handle remaining non local fields (done by bulk_update for now)
        if non_local_fieldnames:
            _rows_updated = qs.bulk_update(objs, non_local_fieldnames, batch_size)
            rows_updated = max(rows_updated, _rows_updated or 0)

    return rows_updated
