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
