from weakref import WeakKeyDictionary
from django.db import transaction, models, connections
from django.db.models.functions import Cast
from django.db.models.expressions import Col
from operator import attrgetter

# typing imports
from django.db.models import Field
from typing import Callable, Dict, Iterable, List, Sequence, Any, Tuple, Union, cast
from django.db.models.sql.compiler import SQLCompiler
from django.db.backends.utils import CursorWrapper
from django.db.backends.base.base import BaseDatabaseWrapper


"""
DB vendor low level interfaces

To register fast update implementations, call:

    register_implementation('vendor', check_function)

where `vendor` is the vendor name as returned by `connection.vendor`.
The check function gets called once (lazy) with `connection` and is meant
to find a suitable implementation (you can provide multiple for different
server versions), either actively by probing against the db server,
or directly if it can be determined upfront.
The check function should return a tuple of (create_sql, prepare_data | None)
for supported backends, or an empty tuple, if the backend is unsupported.

create_sql function:

    def as_sql_xy(
        tname: str,
        pkname: str,
        fields: Sequence[Field],
        placeholders: List[str],
        compiler: SQLCompiler,
        connection: BaseDatabaseWrapper
    ) -> str: ...

    `tname` - name of the target table as pulled from the ORM
    `pkname` - name of the pk field (always the first in a row)
    `fields` - ordered fields as occuring in the data after pk
    `placeholders` - [col][row] placeholders (transposed)
    `compiler` - current update compiler
    `connection` - current write connection

    The placeholders are already enriched from .get_placeholder.
    They come transposed (column based), thus most likely need to be
    transposed back into row based for sql templating.
    The SQL function should return an SQL template with proper placeholders
    applied to do the update from the prepared data list.

prepare_data function:

    def prepare_data_xy(
        data: List[Any],
        width: int,
        height: int
    ) -> List[Any]: ...

    `data` - flat 1-d data list (all values prepared for save)
    `width` - row width
    `height` - column height

    This function is meant to customize the data list, in case more than
    the flat data table preparation is needed. The data is row based,
    and contains field values in [pk] + fields order.
    Return the altered data listing according to the SQL needs.
"""


# Fast update implementations for postgres, sqlite and mysql.


def pq_cast(tname: str, field: Field, compiler: SQLCompiler, connection: Any) -> str:
    """Column type cast for postgres."""
    # TODO: compare to as_postgresql in v4
    return Cast(Col(tname, field), output_field=field).as_sql(compiler, connection)[0]


def as_postgresql(
    tname: str,
    pkname: str,
    fields: Sequence[Field],
    placeholders: List[str],
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
    values = ','.join([f'({",".join(row)})' for row in zip(*placeholders)])
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
    placeholders: List[str],
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
    values = ','.join([f'({",".join(row)})' for row in zip(*placeholders)])
    where = f'"{tname}"."{pkname}"="{dname}"."column1"'
    return f'UPDATE "{tname}" SET {cols} FROM (VALUES {values}) AS "{dname}" WHERE {where}'


def as_sqlite_cte(
    tname: str,
    pkname: str,
    fields: Sequence[Field],
    placeholders: List[str],
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> str:
    """
    sqlite >=3.15 <3.33 does not support yet the FROM VALUES pattern, but has
    CTE support (>=3.8) with row assignment in UPDATE (>=3.15).
    So we can use CTE to construct the values table upfront with UNION ALL,
    and do row level update in the SET clause.

    To limit the updated rows we normally would do a correlated existance test
    in the update WHERE clause:

        WHERE EXISTS (SELECT 1 FROM target WHERE target.pk = cte.pk)

    but this shows very bad performance due to repeated rescanning of the
    values table. We can achieve the same filter condition by providing
    the pks with an IN test:

        WHERE target.pk in (pk1, pk2, ...)

    This sacrifices some bandwidth, but is only ~40% slower than the
    FROM VALUES table join.
    """
    dname = 'd' if tname != 'd' else 'c'
    cols = ', '.join([f'"{f.column}"' for f in fields])
    rows = [','.join(row) for row in zip(*placeholders)]
    values = ' UNION ALL '.join([' SELECT ' + row for row in rows])
    where = f'"{tname}"."{pkname}"={dname}."{pkname}"'
    pks = ','.join(placeholders[0])
    return (
        f'WITH {dname}("{pkname}", {cols}) AS ({values}) '
        f'UPDATE "{tname}" '
        f'SET ({cols}) = (SELECT {cols} FROM {dname} WHERE {where}) '
        f'WHERE "{tname}"."{pkname}" in ({pks})'
    )


def prepare_data_sqlite_cte(data, width, height):
    return data + [data[i] for i in range(0, len(data), width)]


def as_mysql(
    tname: str,
    pkname: str,
    fields: Sequence[Field],
    placeholders: List[str],
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> str:
    """
    For MySQL we use old style SELECT + UNION ALL to create the values table.
    This also keeps it compatible to older MySQL and MariaDB versions.

    Newer db versions have a more direct way to load literal values with
    TVC (MariaDB 10.3.3+) and extended VALUES statement (MySQL 8.0.19+).
    We dont use those anymore, as they dont show a performance improvement,
    while creating much code/branching noise for no good reason.
    """
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'`{tname}`.`{f.column}`=`{dname}`.`{f.column}`' for f in fields)
    colnames = [pkname] + [f.column for f in fields]
    row_phs = [ph for ph in zip(*placeholders)]
    first_row = 'SELECT '
    first_row += ','.join([
        f'{ph} AS `{colname}`'
        for ph, colname in zip(row_phs[0], colnames)
    ])
    later_rows = ' UNION ALL '.join(['SELECT ' + ','.join(ph) for ph in row_phs[1:]])
    values = f'{first_row} UNION ALL {later_rows}' if later_rows else first_row
    where = f'`{tname}`.`{pkname}` = `{dname}`.`{pkname}`'
    return (
        f'UPDATE `{tname}`, ({values}) {dname} '
        f'SET {cols} WHERE {where}'
    )


# Implementation registry.


# memorize fast_update vendor on connection object
SEEN_CONNECTIONS = cast(Dict[BaseDatabaseWrapper, str], WeakKeyDictionary())
CHECKER = {}

def register_implementation(
    vendor: str,
    func: Callable[[BaseDatabaseWrapper], Tuple[Any]]
) -> None:
    """
    Register fast update implementation for db vendor.

    `vendor` is the vendor name as returned by `connection.vendor`.
    `func` is a lazy called function to check support for a certain
    implementation at runtime for `connection`. The function should return
    a tuple of (create_sql, prepare_data | None) for supported backends,
    otherwise an empty tuple (needed to avoid re-eval).
    """
    CHECKER[vendor] = func


def get_impl(conn: BaseDatabaseWrapper) -> str:
    """
    Try to get a fast update implementation for `conn`.
    Calls once the check function of `register_implementation` and
    memorizes its result for `conn`.
    Returns a tuple (create_sql, prepare_data | None) for supported backends,
    otherwise an empty tuple.
    """
    impl = SEEN_CONNECTIONS.get(conn)
    if impl is not None:
        return impl
    check = CHECKER.get(conn.vendor)
    if not check:   # pragma: no cover
        SEEN_CONNECTIONS[conn] = tuple()
        return tuple()
    impl = check(conn) or tuple()   # NOTE: in case check returns something nullish
    SEEN_CONNECTIONS[conn] = impl
    return impl


# Register default db implementations from above.
register_implementation(
    'postgresql',
    lambda _: (as_postgresql, None)
)
register_implementation(
    'sqlite',
    # NOTE: check function does not handle versions <3.15 anymore
    lambda conn: (as_sqlite, None) if conn.Database.sqlite_version_info >= (3, 33)
        else (as_sqlite_cte, prepare_data_sqlite_cte)
)
register_implementation(
    'mysql',
    lambda _: (as_mysql, None)
)


# Update implementation.


def update_from_values(
    c: CursorWrapper,
    tname: str,
    pk_field: Field,
    fields: List[Field],
    counter: int,
    data: List[Any],
    compiler: SQLCompiler,
    connection: BaseDatabaseWrapper
) -> int:
    """
    Generate vendor specific SQL statement and execute it for given data.
    """
    # The following placeholder calc is quite cumbersome:
    # For fast processing we approach the data col-based (transposed)
    # to save runtime for funcion pointer juggling for every single row
    # which is ~90% faster than a more direct row-based evaluation.
    # This is still alot slower than direct flat layouting, but not significant
    # anymore for the total runtime (<<1%, a row-based approach takes 3-7%).
    row_fields = [pk_field] + fields
    row_length = len(row_fields)
    default_placeholder = ['%s'] * counter
    placeholders = [
        ([
            f.get_placeholder(data[i], compiler, connection)
            for i in range(pos, len(data), row_length)
        ] if hasattr(f, 'get_placeholder') else default_placeholder)
        for pos, f in enumerate(row_fields)
    ]
    create_sql, prepare_data = get_impl(connection)
    sql = create_sql(tname, pk_field.column, fields, placeholders, compiler, connection)
    if prepare_data:
        data = prepare_data(data, row_length, counter)
    c.execute(sql, data)
    rows_updated = c.rowcount
    # NOTE: SQLite cannot report correct rowcount from a CTE context
    # as per Python DB API 2.0 (PEP 249) we treat -1 in rowcount
    # as fault and return counter instead
    if rows_updated == -1:
        return counter
    return rows_updated


def fast_update(
    qs: models.QuerySet,
    objs: Sequence[models.Model],
    fieldnames: Iterable[str],
    batch_size: Union[int, None]
) -> int:
    qs._for_write = True
    conn = connections[qs.db]
    model = qs.model

    ## fall back to bulk_update if we dont have a working fast_update impl
    if not get_impl(conn):  # pragma: no cover
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
    batch_size_adjusted = min(batch_size or 2 ** 31, max_batch_size)
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
                if counter >= batch_size_adjusted:
                    rows_updated += update_from_values(
                        c, model._meta.db_table, pk_field, fields,
                        counter, data, compiler, conn
                    )
                    data = []
                    counter = 0
            if data:
                rows_updated += update_from_values(
                    c, model._meta.db_table, pk_field, fields,
                    counter, data, compiler, conn
                )

        # handle remaining non local fields (done by bulk_update for now)
        if non_local_fieldnames:
            _rows_updated = qs.bulk_update(objs, non_local_fieldnames, batch_size)
            rows_updated = max(rows_updated, _rows_updated or 0)

    return rows_updated
