from weakref import WeakKeyDictionary
from django.db import transaction, models, connections
from django.db.utils import ProgrammingError
from operator import attrgetter
import logging

# typing imports
from django.db.models import Field
from typing import Sequence


logger = logging.getLogger(__name__)


# memorize fast_update vendor on connection object
SEEN_CONNECTIONS = WeakKeyDictionary()


def get_vendor(connection):
    vendor = SEEN_CONNECTIONS.get(connection)
    if vendor is not None:
        return vendor
    if connection.vendor == 'postgresql':
        SEEN_CONNECTIONS[connection] = 'postgresql'
        return 'postgresql'
    elif connection.vendor == 'sqlite':
        if not connection.connection:
            with connection.cursor():
                pass
        # grab the module to also work with pysqlite3
        import importlib
        _mod = importlib.import_module(connection.connection.__class__.__module__)
        major, minor, _ = _mod.sqlite_version_info
        if major >= 3 and minor > 32:
            SEEN_CONNECTIONS[connection] = 'sqlite'
            return 'sqlite'
        else:
            logger.warning('unsupported sqlite backend, fast_update will fall back to bulk_update')
            SEEN_CONNECTIONS[connection] = ''
            return ''
    elif connection.vendor == 'mysql':
        try:
            with transaction.atomic(using=connection.alias, savepoint=False), connection.cursor() as c:
                c.execute("SELECT foo.0 FROM (VALUES (0, 1), (1, 'zzz'),(2, 'yyy')) as foo")
            SEEN_CONNECTIONS[connection] = 'mysql'
            return 'mysql'
        except ProgrammingError:
            pass
        try:
            with transaction.atomic(using=connection.alias, savepoint=False), connection.cursor() as c:
                c.execute("SELECT column_1 FROM (VALUES ROW(1, 'zzz'), ROW(2, 'yyy')) as foo")
            SEEN_CONNECTIONS[connection] = 'mysql8'
            return 'mysql8'
        except ProgrammingError:
            pass
        logger.warning('unsupported mysql backend, fast_update will fall back to bulk_update')
        SEEN_CONNECTIONS[connection] = ''
        return ''
    logger.warning('unsupported db backend, fast_update will fall back to bulk_update')
    return ''


def as_postgresql(
    tname: str,
    pkname: str,
    colnames: Sequence[Field],
    count: int
) -> str:
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'"{column}"="{dname}"."{column}"' for column in colnames)
    value = f'({",".join(["%s"] * (len(colnames) + 1))})'
    values = ','.join([value] * count)
    dcols = f'"{pkname}",' + ','.join(f'"{column}"' for column in colnames)
    where = f'"{tname}"."{pkname}"="{dname}"."{pkname}"'
    return f'UPDATE "{tname}" SET {cols} FROM (VALUES {values}) AS "{dname}" ({dcols}) WHERE {where}'


def as_sqlite(
    tname: str,
    pkname: str,
    colnames: Sequence[Field],
    count: int
) -> str:
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'"{column}"="{dname}"."column{i + 2}"' for i, column in enumerate(colnames))
    value = f'({",".join(["%s"] * (len(colnames) + 1))})'
    values = ','.join([value] * count)
    where = f'"{tname}"."{pkname}"="{dname}"."column1"'
    return f'UPDATE "{tname}" SET {cols} FROM (VALUES {values}) AS "{dname}" WHERE {where}'


def as_mysql(
    tname: str,
    pkname: str,
    colnames: Sequence[Field],
    count: int
) -> str:
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'`{column}`={dname}.{i+1}' for i, column in enumerate(colnames))
    value = f'({",".join(["%s"] * (len(colnames) + 1))})'
    values = ",".join([value] * (count + 1))
    on = f'`{tname}`.`{pkname}` = {dname}.0'
    return f'UPDATE `{tname}` INNER JOIN (VALUES {values}) AS {dname} ON {on} SET {cols}'


def as_mysql8(
    tname: str,
    pkname: str,
    colnames: Sequence[Field],
    count: int
) -> str:
    dname = 'd' if tname != 'd' else 'c'
    cols = ','.join(f'`{column}`={dname}.column_{i+1}' for i, column in enumerate(colnames))
    value = f'ROW({",".join(["%s"] * (len(colnames) + 1))})'
    values = ",".join([value] * count)
    on = f'`{tname}`.`{pkname}` = {dname}.column_0'
    return f'UPDATE `{tname}` INNER JOIN (VALUES {values}) AS {dname} ON {on} SET {cols}'



QUERY = {
    'sqlite': as_sqlite,
    'postgresql': as_postgresql,
    'mysql': as_mysql,
    'mysql8': as_mysql8
}


def _update_from_values(c, vendor, tname, pkname, colnames, counter, data):
    # TODO: cache sql for equal counter
    sql = QUERY[vendor](tname, pkname, colnames, counter)
    if vendor == 'mysql':
        # mysql needs data patch with (0,1,2,...) as first VALUES entry
        data = list(range(len(colnames) + 1)) + data
    c.execute(sql, data)
    return c.rowcount


def _fast_update(connection, model, objs, fields, batch_size=None):
    vendor = get_vendor(connection)
    if not vendor:
        return model.objects.using(connection.alias).bulk_update(objs, fields, batch_size)
    
    # reject pk in fields
    pk_field = model._meta.pk
    pk_attname = pk_field.attname
    if pk_attname in fields:
        raise ValueError('fast_update() cannot be used with primary key fields.')
    # filter all non model local fields --> still handled by bulk_update
    non_local_fieldnames = []
    local_fieldnames = []
    for fieldname in fields:
        # FIXME: pk fieldname should never be listed in fields
        if model._meta.get_field(fieldname) not in model._meta.local_fields:
            non_local_fieldnames.append(fieldname)
        else:
            local_fieldnames.append(fieldname)
    
    # avoid more expensive doubled updates
    if non_local_fieldnames and len(local_fieldnames) < 2:
        return model.objects.using(connection.alias).bulk_update(objs, fields, batch_size)
    
    rows_updated = 0
    with transaction.atomic(using=connection.alias, savepoint=False):
        if local_fieldnames:
            max_batch_size = connection.ops.bulk_batch_size(['pk'] + fields, objs)
            batch_size = min(batch_size or 2 ** 31, max_batch_size)
            tablename = model._meta.db_table
            pk_colname = pk_field.column
            fields = [model._meta.get_field(f) for f in local_fieldnames]
            colnames = [f.column for f in fields]
            get = attrgetter(pk_attname, *(f.attname for f in fields))
            prep_save = [pk_field.get_db_prep_save] + [f.get_db_prep_save for f in fields]
            data = []
            counter = 0
            with connection.cursor() as c:
                # FIXME: Should we raise on expression-like values?
                for o in objs:
                    counter += 1
                    data += [p(v, connection) for p, v in zip(prep_save, get(o))]
                    if counter >= batch_size:
                        rows_updated += _update_from_values(
                            c, vendor, tablename, pk_colname, colnames, counter, data)
                        data = []
                        counter = 0
                if data:
                    rows_updated += _update_from_values(
                        c, vendor, tablename, pk_colname, colnames, counter, data)

        if non_local_fieldnames:
            _rows_updated = model.objects.using(connection.alias).bulk_update(objs, non_local_fieldnames, batch_size)
            rows_updated = max(rows_updated, _rows_updated or 0)
    return rows_updated


class FastUpdateQuerySet(models.QuerySet):
    def fast_update(self, objs, fields, batch_size=None):
        self._for_write = True
        return _fast_update(connections[self.db], self.model, objs, fields, batch_size)
    fast_update.alters_data = True


class FastUpdateManager(models.Manager.from_queryset(FastUpdateQuerySet)):
    pass
