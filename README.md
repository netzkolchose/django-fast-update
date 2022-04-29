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
but make sure not to feed something totally off).


### Compatibility ###

`fast_update` is known to work with these database versions:

- SQLite 3.15+
- PostgreSQL
- MariaDB 10.2+
- MySQL 5.7+

For unsupported database backends or outdated versions `fast_update` will fall back to `bulk_update`.
(It is possible to register fast update implementations for other db vendors with `register_implementation`.
Plz see `fast_update/fast.py` for more details.)

Note that with `fast_update` f-expressions cannot be used anymore.
This is a design decision to not penalize update performance by some swiss-army-knife functionality.
If you have f-expressions in your update data, consider re-grouping the update steps and update those
fields with `update` or `bulk_update` instead.


### copy_update ###

This is a PostgreSQL only update implementation based on `COPY FROM`. This runs even faster
than `fast_update` for medium to big changesets (but tends to be slower than `fast_update` for <100 objects).

`copy_update` follows the same interface idea as `bulk_update` and `fast_update`, minus a `batch_size`
argument (data is always transferred in one big batch). It can be used likewise from the `FastUpdateManager`.
`copy_update` also has no support for f-expressions.

**Note** `copy_update` will probably never leave the alpha/PoC-state, as psycopg3 brings great COPY support,
which does a more secure value conversion and has a very fast C-version.


### Status ###

Currently beta, still some TODOs left (including better docs).

The whole package is tested with Django 3.2 & 4.0 on Python 3.8 & 3.10.


### Performance ###

There is a management command in the example app testing performance of updates on the `FieldUpdate`
model (`./manange.py perf`).

Here are some numbers from my laptop (tested with `settings.DEBUG=False`,
db engines freshly bootstrapped from docker as mentioned in `settings.py`):


| Postgres | bulk_update | fast_update  | bulk/fast | copy_update | bulk/copy | fast/copy |
|----------|-------------|--------------|-----------|-------------|-----------|-----------|
| 10       | 0.0471      | 0.0044       | 10.7      | 0.0083      | 5.7       | 0.5       |
| 100      | 0.4095      | 0.0222       | 18.4      | 0.0216      | 18.9      | 1.0       |
| 1000     | 4.4909      | 0.1571       | 28.6      | 0.0906      | 49.6      | 1.7       |
| 10000    | 86.89       | 1.49         | 58.3      | 0.70        | 124.1     | 2.1       |

| SQLite | bulk_update | fast_update  | ratio |
|--------|-------------|--------------|-------|
| 10     | 0.0443      | 0.0018       | 24.6  |
| 100    | 0.4408      | 0.0108       | 40.8  |
| 1000   | 4.0178      | 0.0971       | 41.4  |
| 10000  | 40.90       | 0.97         | 42.2  |

| MariaDB | bulk_update | fast_update  | ratio |
|---------|-------------|--------------|-------|
| 10      | 0.0448      | 0.0049       | 9.1   |
| 100     | 0.4069      | 0.0252       | 16.1  |
| 1000    | 5.0570      | 0.1759       | 28.7  |
| 10000   | 139.20      | 1.74         | 80.0  |

| MySQL8 | bulk_update | fast_update  | ratio |
|--------|-------------|--------------|-------|
| 10     | 0.0442      | 0.0055       | 8.0   |
| 100    | 0.4132      | 0.0278       | 14.9  |
| 1000   | 5.2495      | 0.2115       | 24.8  |
| 10000  | 136.61      | 1.99         | 68.6  |


`fast_update` is at least 8 times faster than `bulk_update`, and keeps making ground for bigger changesets.
This indicates different runtime complexity. `fast_update` grows almost linear for very big numbers of rows
(tested during some perf series against `copy_update` up to 10M), while `bulk_update` grows much faster
(looks quadratic to me, did not further investigate this).

For very big changesets `copy_update` is the clear winner, and even shows a substantial increase in updated rows/s
(within my test range, as upper estimate this of course cannot grow slower than linear,
as the data pumping will saturate to linear).
