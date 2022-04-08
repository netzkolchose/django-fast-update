[![test](https://github.com/netzkolchose/django-fast-update/actions/workflows/django.yml/badge.svg?branch=master)](https://github.com/netzkolchose/django-fast-update/actions/workflows/django.yml)
[![Coverage Status](https://coveralls.io/repos/github/netzkolchose/django-fast-update/badge.svg?branch=master)](https://coveralls.io/github/netzkolchose/django-fast-update?branch=master)


## django-fast-update ##

Faster db updates using `UPDATE FROM VALUES` sql variants.

### fast_update ###

`fast_update` is meant to be used as `bulk_update` replacement.


#### Example Usage ####

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


#### Compatibility ####

`fast_update` is implemented for these database backends:
- SQLite 3.33+
- PostgreSQL
- MariaDB 10.3.3+
- MySQL 8.0.19+

Note that with `fast_update` f-expressions cannot be used anymore.
This is a design decision to not penalize update performance by some swiss-army-knife functionality.
If you have f-expressions in your update data, consider re-grouping the update steps and update those
fields with `update` or `bulk_update` instead.


### copy_update ###

This is a PostgreSQL only update implementation based on `COPY FROM`. This runs even faster
than `fast_update` for medium to big changesets.

Note that this will probably never leave the alpha/PoC-state, as psycopg3 brings great COPY support,
which does a more secure value conversion and even runs faster in the C-version.

TODO - describe usage and limitations...


### Status ###

Currently alpha, left to do:
- finish `copy_update` (array null cascading, some tests)
- some better docs


### Performance ###

There is a management command in the example app testing performance of updates on the `FieldUpdate` model.
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
