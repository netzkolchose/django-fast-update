# Generated by Django 3.2.12 on 2022-04-06 14:04

import datetime
from django.db import migrations, models
from django.utils.timezone import utc
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('postgres_tests', '0002_fieldupdatenotnull'),
    ]

    operations = [
        migrations.AlterField(
            model_name='fieldupdatenotnull',
            name='f_datetime',
            field=models.DateTimeField(default=datetime.datetime(1000, 1, 1, 0, 0, tzinfo=utc)),
        ),
        migrations.AlterField(
            model_name='fieldupdatenotnull',
            name='f_ip',
            field=models.GenericIPAddressField(default='127.0.0.1'),
        ),
        migrations.AlterField(
            model_name='fieldupdatenotnull',
            name='f_uuid',
            field=models.UUIDField(default=uuid.UUID('cb36b1fa-976a-47ae-bf8b-42efa4fa59de')),
        ),
    ]