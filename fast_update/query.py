from django.db import connections
from django.db.models import QuerySet, Model, Manager
from django.db.utils import NotSupportedError
from typing import Any, Dict, Optional, Sequence

from .fast import fast_update


def sanity_check(model, objs, fields, batch_size):
    # basic sanity checks (most taken from bulk_update)
    if batch_size is not None and batch_size < 0:
        raise ValueError('Batch size must be a positive integer.')
    if not fields:
        raise ValueError('Field names must be given to fast_update().')
    if not objs:
        return 0
    if any(obj.pk is None for obj in objs):
        raise ValueError('All fast_update() objects must have a primary key set.')
    fields = [model._meta.get_field(name) for name in fields]
    if any(not f.concrete or f.many_to_many for f in fields):
        raise ValueError('fast_update() can only be used with concrete fields.')
    if any(f.primary_key for f in fields):
        raise ValueError('fast_update() cannot be used with primary key fields.')
    for obj in objs:
        # FIXME: django main has an additional argument 'fields'
        obj._prepare_related_fields_for_save(operation_name='fast_update')
        # additionally raise on f-expression
        for field in fields:
            attr = getattr(obj, field.attname)
            if hasattr(attr, 'resolve_expression'):
                raise ValueError('fast_update() cannot be used with f-expressions.')
    return fields


class FastUpdateQuerySet(QuerySet):
    def fast_update(
        self,
        objs: Sequence[Model],
        fields: Sequence[str],
        batch_size: Optional[int] = None
    ) -> int:
        """
        TODO...
        """
        objs = tuple(objs)
        fields = set(fields or [])
        sanity_check(self.model, objs, fields, batch_size)
        return fast_update(self, objs, fields, batch_size)

    fast_update.alters_data = True

    def copy_update(
        self,
        objs: Sequence[Model],
        fields: Sequence[str],
        field_encoders: Optional[Dict[str, Any]] = None,
        encoding: Optional[str] = None
    ) -> int:
        """
        TODO...
        """
        self._for_write = True
        connection = connections[self.db]
        if connection.vendor != 'postgresql':
            raise NotSupportedError(
                f'copy_update() is not supported on "{connection.vendor}" backend')
        from .copy import copy_update
        objs = tuple(objs)
        fields = set(fields or [])
        sanity_check(self.model, objs, fields, 123)
        return copy_update(self, objs, fields, field_encoders, encoding)
    
    copy_update.alters_data = True


class FastUpdateManager(Manager.from_queryset(FastUpdateQuerySet)):
    pass
