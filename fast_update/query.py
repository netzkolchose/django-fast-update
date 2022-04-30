from django.db import connections
from django.db.models import QuerySet, Model, Manager
from django.db.utils import NotSupportedError
from typing import Any, Dict, Iterable, Optional, Sequence, Type

from .fast import fast_update


def sanity_check(
    model: Type[Model],
    objs: Iterable[Model],
    fields: Iterable[str],
    op: str,
    batch_size: Optional[int] = None
) -> None:
    # basic sanity checks (most taken from bulk_update)
    if batch_size is not None and batch_size < 0:
        raise ValueError('Batch size must be a positive integer.')
    if not fields:
        raise ValueError(f'Field names must be given to {op}.')
    pks = set(obj.pk for obj in objs)
    if len(pks) < len(objs):
        raise ValueError(f'{op} cannot update duplicates.')
    if None in pks:
        raise ValueError(f'All {op} objects must have a primary key set.')
    fields_ = [model._meta.get_field(name) for name in fields]
    if any(not f.concrete or f.many_to_many for f in fields_):
        raise ValueError(f'{op} can only be used with concrete fields.')
    if any(f.primary_key for f in fields_):
        raise ValueError(f'{op} cannot be used with primary key fields.')
    for obj in objs:
        # TODO: This is really heavy in the runtime books, any elegant way to speedup?
        # TODO: django main has an additional argument 'fields' (saves some runtime?)
        obj._prepare_related_fields_for_save(operation_name='fast_update')
        # additionally raise on f-expression
        for field in fields_:
            # TODO: use faster attrgetter
            if hasattr(getattr(obj, field.attname), 'resolve_expression'):
                raise ValueError(f'{op} cannot be used with f-expressions.')


class FastUpdateQuerySet(QuerySet):
    def fast_update(
        self,
        objs: Iterable[Model],
        fields: Iterable[str],
        batch_size: Optional[int] = None
    ) -> int:
        """
        Faster alternative for ``bulk_update`` with the same method signature.

        Due to the way the update works internally with constant VALUES tables,
        f-expressions cannot be used anymore. Beside that it has similar
        restrictions as ``bulk_update`` (e.g. primary keys cannot be updated).

        The internal implementation relies on recent versions of database
        backends and will fall back to ``bulk_update`` if the backend is not
        supported. It will also invoke ``bulk_update`` for non-local fields
        (e.g. for multi-table inheritance).

        ``batch_size`` can be set to much higher values than typically
        for ``bulk_update`` (if needed at all).

        Returns the number of affected rows.
        """
        if not objs:
            return 0
        objs = tuple(objs)
        fields_ = set(fields or [])
        sanity_check(self.model, objs, fields_, 'fast_update()', batch_size)
        return fast_update(self, objs, fields_, batch_size)

    fast_update.alters_data = True

    def copy_update(
        self,
        objs: Iterable[Model],
        fields: Iterable[str],
        field_encoders: Optional[Dict[str, Any]] = None,
        encoding: Optional[str] = None
    ) -> int:
        """
        PostgreSQL only method (raises an exception on any other backend)
        to update a large amount of model instances via COPY FROM.
        The method follows the same interface idea of ``bulk_update`` or ``fast_update``,
        but will perform much better for bigger updates, even than ``fast_update``.

        Other than for ``fast_update``, there is no ``batch_size`` argument anymore,
        as the update is always done in one single big batch by copying the data into
        a temporary table and run the update from there.

        For the data transport postgres' TEXT format is used. For this the field values
        get encoded by special encoders. The encoders are globally registered for
        django's standard field types (works similar to `get_db_prep_value`).
        With ``field_encoders`` custom encoders can be attached to update fields
        for a single call. This might come handy for additional conversion work or
        further speedup by omitting the base type checks of the default encoders
        (do this only if the data was checked by other means, otherwise malformed
        updates may happen).

        ``encoding`` overwrites the text encoding used in the COPY FROM transmission
        (default is psycopg's connection encoding).

        Returns the number of affected rows.

        NOTE: The underlying implementation is only a PoC and probably will be replaced
        soon by the much safer and superior COPY support of psycopg3.
        """
        self._for_write = True
        connection = connections[self.db]
        if connection.vendor != 'postgresql':
            raise NotSupportedError(
                f'copy_update() is not supported on "{connection.vendor}" backend')
        from .copy import copy_update   # TODO: better in conditional import?
        if not objs:
            return 0
        objs = tuple(objs)
        fields_ = set(fields or [])
        sanity_check(self.model, objs, fields_, 'copy_update()')
        return copy_update(self, objs, fields_, field_encoders, encoding)
    
    copy_update.alters_data = True


class FastUpdateManager(Manager.from_queryset(FastUpdateQuerySet)):
    pass
