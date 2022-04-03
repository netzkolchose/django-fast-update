from django.db import models

from fast_update.query import FastUpdateManager

# Create your models here.
class Foo(models.Model):
    objects = FastUpdateManager()
    f1 = models.IntegerField(default=1)
    f2 = models.IntegerField(default=2)
    f3 = models.IntegerField(default=3)
    f4 = models.IntegerField(default=4)
    f5 = models.IntegerField(default=5)
    f6 = models.IntegerField(default=6)
    f7 = models.IntegerField(default=7)
    f8 = models.IntegerField(default=8)
    f9 = models.IntegerField(default=9)
    f10 = models.IntegerField(default=10)


class Child(models.Model):
    objects = FastUpdateManager()


class Parent(models.Model):
    objects = FastUpdateManager()
    child = models.ForeignKey(Child, on_delete=models.CASCADE, null=True)


class BinaryModel(models.Model):
    objects = FastUpdateManager()
    field = models.BinaryField(null=True)


class FieldUpdate(models.Model):
    objects = FastUpdateManager()
    f_biginteger = models.BigIntegerField(null=True)
    f_binary = models.BinaryField(null=True)
    f_boolean = models.BooleanField(null=True)
    f_char = models.CharField(max_length=32, null=True)
    f_date = models.DateField(null=True)
    f_datetime = models.DateTimeField(null=True)
    f_decimal = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    f_duration = models.DurationField(null=True)
    f_email = models.EmailField(null=True)
    f_float = models.FloatField(null=True)
    f_integer = models.IntegerField(null=True)
    f_ip = models.GenericIPAddressField(null=True)
    f_json = models.JSONField(null=True)
    f_slug = models.SlugField(null=True)
    f_smallinteger = models.SmallIntegerField(null=True)
    f_text = models.TextField(null=True)
    f_time = models.TimeField(null=True)
    f_url = models.URLField(null=True)
    f_uuid = models.UUIDField(null=True)
