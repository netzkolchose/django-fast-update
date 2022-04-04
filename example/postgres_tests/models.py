from django.db import models
from django.contrib.postgres.fields import (ArrayField, HStoreField,
    IntegerRangeField, DateTimeRangeField, DateRangeField)
from fast_update.query import FastUpdateManager


class PostgresFields(models.Model):
    objects = FastUpdateManager()
    int1d = ArrayField(models.IntegerField(null=True), null=True)
    int2d = ArrayField(ArrayField(models.IntegerField(null=True), null=True), null=True)
    txt1d = ArrayField(models.CharField(max_length=32, null=True), null=True)
    txt2d = ArrayField(ArrayField(models.CharField(max_length=32, null=True), null=True), null=True)
    hstore = HStoreField(null=True)
    int_r = IntegerRangeField(null=True)
    dt_r = DateTimeRangeField(null=True)
    date_r = DateRangeField(null=True)
