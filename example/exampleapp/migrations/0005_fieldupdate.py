# Generated by Django 3.2.12 on 2022-04-03 10:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('exampleapp', '0004_alter_binarymodel_field'),
    ]

    operations = [
        migrations.CreateModel(
            name='FieldUpdate',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('f_biginteger', models.BigIntegerField(null=True)),
                ('f_binary', models.BinaryField(null=True)),
                ('f_boolean', models.BooleanField(null=True)),
                ('f_char', models.CharField(max_length=32, null=True)),
                ('f_date', models.DateField(null=True)),
                ('f_datetime', models.DateTimeField(null=True)),
                ('f_decimal', models.DecimalField(decimal_places=2, max_digits=10, null=True)),
                ('f_duration', models.DurationField(null=True)),
                ('f_email', models.EmailField(max_length=254, null=True)),
                ('f_float', models.FloatField(null=True)),
                ('f_integer', models.IntegerField(null=True)),
                ('f_ip', models.GenericIPAddressField(null=True)),
                ('f_json', models.JSONField(null=True)),
                ('f_slug', models.SlugField(null=True)),
                ('f_smallinteger', models.SmallIntegerField(null=True)),
                ('f_text', models.TextField(null=True)),
                ('f_time', models.TimeField(null=True)),
                ('f_url', models.URLField(null=True)),
                ('f_uuid', models.UUIDField(null=True)),
            ],
        ),
    ]
