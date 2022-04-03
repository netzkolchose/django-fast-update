[![test](https://github.com/netzkolchose/django-fast-update/actions/workflows/django.yml/badge.svg?branch=master)](https://github.com/netzkolchose/django-fast-update/actions/workflows/django.yml)
[![Coverage Status](https://coveralls.io/repos/github/netzkolchose/django-fast-update/badge.svg?branch=master)](https://coveralls.io/github/netzkolchose/django-fast-update?branch=master)


## django-fast-update ##

Faster db updates using `UPDATE FROM VALUES` sql variants.

### `fast_update` ###

`fast_update` is intended to be used as `bulk_update` replacement.


#### Example Usage ####

With attaching `FastUpdateManager` as a manager to your model, `fast_update`
can be used instead of `bulk_update` like this:

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
If you have f-expressions in your update data, consider re-grouping the update steps and update
fields with expression values with `update` or `bulk_update` instead.


### `copy_update` ###

The package also contains an early draft `copy_update`, for table updates with `COPY FROM`
for PostgreSQL. This is not yet fully implemented, it still misses several value encoders
and field guards.


### Status ###

Currently early alpha, missing lots of things:
- `fast_update`: ArrayField / HStore / range field tests for PostgreSQL
- `copy_update`: currently incomplete draft (unusable)
- better docs
