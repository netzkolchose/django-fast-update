# Generated by Django 3.2.12 on 2022-04-07 21:12

import django.contrib.postgres.fields
from django.db import migrations, models
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('postgres_tests', '0003_auto_20220406_1404'),
    ]

    operations = [
        migrations.CreateModel(
            name='FieldUpdateArray',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('f_biginteger', django.contrib.postgres.fields.ArrayField(base_field=models.BigIntegerField(null=True), null=True, size=None)),
                ('f_binary', django.contrib.postgres.fields.ArrayField(base_field=models.BinaryField(null=True), null=True, size=None)),
                ('f_boolean', django.contrib.postgres.fields.ArrayField(base_field=models.BooleanField(null=True), null=True, size=None)),
                ('f_char', django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=32, null=True), null=True, size=None)),
                ('f_date', django.contrib.postgres.fields.ArrayField(base_field=models.DateField(null=True), null=True, size=None)),
                ('f_datetime', django.contrib.postgres.fields.ArrayField(base_field=models.DateTimeField(null=True), null=True, size=None)),
                ('f_decimal', django.contrib.postgres.fields.ArrayField(base_field=models.DecimalField(decimal_places=2, max_digits=10, null=True), null=True, size=None)),
                ('f_duration', django.contrib.postgres.fields.ArrayField(base_field=models.DurationField(null=True), null=True, size=None)),
                ('f_email', django.contrib.postgres.fields.ArrayField(base_field=models.EmailField(max_length=254, null=True), null=True, size=None)),
                ('f_float', django.contrib.postgres.fields.ArrayField(base_field=models.FloatField(null=True), null=True, size=None)),
                ('f_integer', django.contrib.postgres.fields.ArrayField(base_field=models.IntegerField(null=True), null=True, size=None)),
                ('f_ip', django.contrib.postgres.fields.ArrayField(base_field=models.GenericIPAddressField(null=True), null=True, size=None)),
                ('f_json', django.contrib.postgres.fields.ArrayField(base_field=models.JSONField(null=True), null=True, size=None)),
                ('f_slug', django.contrib.postgres.fields.ArrayField(base_field=models.SlugField(null=True), null=True, size=None)),
                ('f_smallinteger', django.contrib.postgres.fields.ArrayField(base_field=models.SmallIntegerField(null=True), null=True, size=None)),
                ('f_text', django.contrib.postgres.fields.ArrayField(base_field=models.TextField(null=True), null=True, size=None)),
                ('f_time', django.contrib.postgres.fields.ArrayField(base_field=models.TimeField(null=True), null=True, size=None)),
                ('f_url', django.contrib.postgres.fields.ArrayField(base_field=models.URLField(null=True), null=True, size=None)),
                ('f_uuid', django.contrib.postgres.fields.ArrayField(base_field=models.UUIDField(null=True), null=True, size=None)),
                ('f_biginteger2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.BigIntegerField(null=True), null=True, size=None), null=True, size=None)),
                ('f_binary2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.BinaryField(null=True), null=True, size=None), null=True, size=None)),
                ('f_boolean2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.BooleanField(null=True), null=True, size=None), null=True, size=None)),
                ('f_char2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=32, null=True), null=True, size=None), null=True, size=None)),
                ('f_date2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.DateField(null=True), null=True, size=None), null=True, size=None)),
                ('f_datetime2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.DateTimeField(null=True), null=True, size=None), null=True, size=None)),
                ('f_decimal2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.DecimalField(decimal_places=2, max_digits=10, null=True), null=True, size=None), null=True, size=None)),
                ('f_duration2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.DurationField(null=True), null=True, size=None), null=True, size=None)),
                ('f_email2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.EmailField(max_length=254, null=True), null=True, size=None), null=True, size=None)),
                ('f_float2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.FloatField(null=True), null=True, size=None), null=True, size=None)),
                ('f_integer2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.IntegerField(null=True), null=True, size=None), null=True, size=None)),
                ('f_ip2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.GenericIPAddressField(null=True), null=True, size=None), null=True, size=None)),
                ('f_json2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.JSONField(null=True), null=True, size=None), null=True, size=None)),
                ('f_slug2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.SlugField(null=True), null=True, size=None), null=True, size=None)),
                ('f_smallinteger2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.SmallIntegerField(null=True), null=True, size=None), null=True, size=None)),
                ('f_text2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.TextField(null=True), null=True, size=None), null=True, size=None)),
                ('f_time2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.TimeField(null=True), null=True, size=None), null=True, size=None)),
                ('f_url2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.URLField(null=True), null=True, size=None), null=True, size=None)),
                ('f_uuid2', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.UUIDField(null=True), null=True, size=None), null=True, size=None)),
            ],
        ),
        migrations.AlterField(
            model_name='fieldupdatenotnull',
            name='f_uuid',
            field=models.UUIDField(default=uuid.UUID('5b13de5e-8f15-44ee-a31a-a842f0d3ef05')),
        ),
    ]
