# Generated by Django 3.2.12 on 2022-04-01 10:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('exampleapp', '0003_binarymodel'),
    ]

    operations = [
        migrations.AlterField(
            model_name='binarymodel',
            name='field',
            field=models.BinaryField(null=True),
        ),
    ]
