from django.db import models
from django.contrib.postgres.fields import (ArrayField, HStoreField,
    IntegerRangeField, DateTimeRangeField, DateRangeField)
from fast_update.query import FastUpdateManager
from datetime import date, datetime, timedelta, time
from decimal import Decimal
from uuid import uuid4
import pytz

fixed_uuid = uuid4()

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


class FieldUpdateNotNull(models.Model):
    objects = FastUpdateManager()
    f_biginteger = models.BigIntegerField(default=0)
    f_binary = models.BinaryField(default=b'')
    f_boolean = models.BooleanField(default=False)
    f_char = models.CharField(max_length=32, default='')
    f_date = models.DateField(default=date(1000, 1, 1))
    f_datetime = models.DateTimeField(default=datetime(1000,1,1,0,0,0, tzinfo=pytz.UTC))
    f_decimal = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal(0))
    f_duration = models.DurationField(default=timedelta(days=1))
    f_email = models.EmailField(default='')
    f_float = models.FloatField(default=0.0)
    f_integer = models.IntegerField(default=0)
    f_ip = models.GenericIPAddressField(default='127.0.0.1')
    f_json = models.JSONField(default=dict)
    f_slug = models.SlugField(default='')
    f_smallinteger = models.SmallIntegerField(default=0)
    f_text = models.TextField(default='')
    f_time = models.TimeField(default=time(1, 0, 0))
    f_url = models.URLField(default='')
    f_uuid = models.UUIDField(default=fixed_uuid)


class CustomField(models.Field): pass
