from collections import defaultdict
from operator import attrgetter
from django.db.models import QuerySet
from django.db.transaction import atomic
import math

from typing import Sequence, Any, List, Callable


"""
Cost Prediction for Updates

To decide, whether the merge attempt saves any runtime,
we do a cost prediction with these assumptions:

- any value transfer costs 1w
- an UPDATE(1) call costs 10w and grows in O(lb n) for n pks

work in flat mode:
Flat means, that we transfer values for each object in a separate UPDATE.
    ==> n * UPDATE(1) + n * field_count (for n updates)

work in merged mode:
In merged mode we sum the costs of two update components:
    flat residues  ==> n * UPDATE(1) + counted_values (for n flat residues)
    merged updates ==> n * UPDATE(m) + counted_values (for n updates, m pks)

If the ratio of merged/flat work is below 0.8, merged updates get applied.

The predictor works close enough in local tests with sqlite and postgres,
but will hugely be skewed by several factors:
- weight of field types (an integer is cheaper than a long string)
- DB latency (with higher latency merge will be underestimated)

Both, type weighing and latency measuring is def. out of scope,
thus the predictor gives only a conversative estimate preferring flat mode.
"""


def upd_pk_work(n):
    return 10 + math.log2(n)
UDP_1 = upd_pk_work(1)


def predictor(objs, fields, merged_updates, residues):
    # flat work
    flat_work = (len(fields) + UDP_1) * len(objs)

    # flat residues
    uh_work = len(residues.keys()) * UDP_1 + sum(map(len, residues.values()))

    # merged updates
    mg_work = (sum(upd_pk_work(len(o)) for o in merged_updates.keys())
               + sum(map(len, merged_updates.values())))

    return (uh_work + mg_work) / flat_work


def _flat(
    qs: QuerySet,
    objs: Sequence[Any],
    fields: List[str]
) -> None:
    qs._for_write = True
    get_values = attrgetter(*fields)
    rows_updated = 0
    if len(fields) == 1:
        for o in objs:
            rows_updated += qs.filter(pk=o.pk).update(
                **{fields[0]: get_values(o)})
    else:
        for o in objs:
            rows_updated += qs.filter(pk=o.pk).update(
                **dict(zip(fields, get_values(o))))
    return rows_updated


def _merge_values(
    objs: Sequence[Any],
    fields: List[str]
):
    merged_pks = defaultdict(lambda: defaultdict(list))
    flat_residues = defaultdict(dict)

    # 1. aggregate pks under equal hashable values
    for fieldname in fields:
        accu = merged_pks[fieldname]
        get_value = attrgetter(fieldname)
        for o in objs:
            value = get_value(o)
            try:
                accu[value].append(o.pk)
            except TypeError:
                flat_residues[o.pk][fieldname] = value
        # TODO: should we bail out early, if merge looks bad?
        # currently the full merge before prediction costs <10%

    # 2. aggregate fields under pk groups
    merged_updates = defaultdict(dict)
    for fieldname, pkdata in merged_pks.items():
        for value, pks in pkdata.items():
            if len(pks) == 1:
                # transfer to residues to allow merge over fields there
                flat_residues[list(pks)[0]][fieldname] = value
            else:
                merged_updates[frozenset(pks)][fieldname] = value
    
    return merged_updates, flat_residues


def _merged(
    qs: QuerySet,
    objs: Sequence[Any],
    fields: List[str]
) -> None:
    qs._for_write = True
    
    merged_updates, flat_residues = _merge_values(objs, fields)

    if predictor(objs, fields, merged_updates, flat_residues) < 0.8:
        # exec merged updates
        for pks, data in merged_updates.items():
            qs.filter(pk__in=pks).update(**data)
        for pk, data in flat_residues.items():
            qs.filter(pk=pk).update(**data)
        return len(objs)

    # if predictor is worse, use flat instead
    return _flat(qs, objs, fields)


def group_fields(model, fieldnames):
    field_groups = defaultdict(list)
    field_groups[model]
    for fieldname in fieldnames:
        field = model._meta.get_field(fieldname)
        field_groups[field.model].append(fieldname)
    return field_groups


def _update(
    func: Callable[[QuerySet, Sequence[Any], List[str]], int],
    qs: QuerySet,
    objs: Sequence[Any],
    fieldnames: Sequence[str],
    unfiltered: bool = False
) -> None:
    qs._for_write = True
    rows_updated = 0
    with atomic(using=qs.db, savepoint=False):
        if not unfiltered and qs.query.where:
          return func(qs, objs, fieldnames)
        for model, local_fields in group_fields(qs.model, fieldnames).items():
            if local_fields:
                ru = func(model._base_manager.using(qs.db), objs, local_fields)
                rows_updated = max(rows_updated, ru)
        return rows_updated


def flat_update(
    qs: QuerySet,
    objs: Sequence[Any],
    fieldnames: Sequence[str],
    unfiltered: bool = False
) -> None:
    return _update(_flat, qs, objs, fieldnames, unfiltered)


def merged_update(
    qs: QuerySet,
    objs: Sequence[Any],
    fieldnames: Sequence[str],
    unfiltered: bool = False
) -> None:
    if len(objs) < 3:
        return _update(_flat, qs, objs, fieldnames, unfiltered)
    return _update(_merged, qs, objs, fieldnames, unfiltered)
