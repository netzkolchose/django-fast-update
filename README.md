[![test](https://github.com/netzkolchose/django-fast-update/actions/workflows/django.yml/badge.svg?branch=master)](https://github.com/netzkolchose/django-fast-update/actions/workflows/django.yml)
[![Coverage Status](https://coveralls.io/repos/github/netzkolchose/django-fast-update/badge.svg?branch=master)](https://coveralls.io/github/netzkolchose/django-fast-update?branch=master)


## django-fast-update ##

Faster db updates using `UPDATE FROM VALUES` sql variants.

### Installation & Usage ###

Run `pip install django-fast-update` and place `fast_update` in INSTALLED_APPS.

With attaching `FastUpdateManager` as a manager to your model, `fast_update`
can be used instead of `bulk_update`, e.g.:

```python
from django.db import models
from fast_update.query import FastUpdateManager

class MyModel(models.Model):
    objects = FastUpdateManager()
    field_a = ...
    field_b = ...
    field_c = ...


# to update multiple instances at once:
MyModel.objects.fast_update(bunch_of_instances, ['field_a', 'field_b', 'field_c'])
```

Alternatively `fast.fast_update` can be used directly with a queryset as first argument
(Warning - this skips most sanity checks with up to 30% speed gain,
so make sure not to feed something totally off).


### Compatibility ###

`fast_update` is known to work with these database versions:

- SQLite 3.15+
- PostgreSQL
- MariaDB 10.2+
- MySQL 5.7+

For unsupported database backends or outdated versions `fast_update` will fall back to `bulk_update`.
(It is possible to register fast update implementations for other db vendors with `register_implementation`.
See `fast_update/fast.py` for more details.)

Note that with `fast_update` f-expressions cannot be used anymore.
This is a design decision to not penalize update performance by some swiss-army-knife functionality.
If you have f-expressions in your update data, consider re-grouping the update steps and update those
fields with `update` or `bulk_update` instead.

Other than with `bulk_update` duplicates in a changeset are not allowed and will raise.
This is mainly a safety guard to not let slip through duplicates, where the final update state
would be undetermined or directly depend on the database's compatibility.


### copy_update ###

This is a PostgreSQL only update implementation based on `COPY FROM`. This runs even faster
than `fast_update` for medium to big changesets (but tends to be slower than `fast_update` for <100 objects).

`copy_update` follows the same interface idea as `bulk_update` and `fast_update`, minus a `batch_size`
argument (data is always transferred in one big batch). It can be used likewise from the `FastUpdateManager`.
`copy_update` also has no support for f-expressions, also duplicates will raise.

**Note** `copy_update` will probably never leave the alpha/PoC-state, as psycopg3 brings great COPY support,
which does a more secure value conversion and has a very fast C-version.


### Status ###

Currently beta, still some TODOs left.

The whole package is tested with Django 3.2 & 4.0 on Python 3.8 & 3.10.


### Performance ###

There is a management command in the example app testing performance of updates on the `FieldUpdate`
model (`./manage.py perf`).

`fast_update` is at least 8 times faster than `bulk_update`, and keeps making ground for bigger changesets.
This indicates different runtime complexity. `fast_update` grows almost linear for very big numbers of rows
(tested during some perf series against `copy_update` up to 10M), while `bulk_update` grows much faster
(looks quadratic to me, which can be lowered to linear by applying a proper `batch_size`,
but it stays very steep compared to `fast_update`).

For very big changesets `copy_update` is the clear winner, and even shows a substantial increase in updated rows/s
(within my test range, as upper estimate this of course cannot grow slower than linear,
as the data pumping will saturate to linear).
