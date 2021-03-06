# Generated by Django 3.2.12 on 2022-04-03 13:52

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('exampleapp', '0005_fieldupdate'),
    ]

    operations = [
        migrations.CreateModel(
            name='MultiBase',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('b1', models.IntegerField(null=True)),
                ('b2', models.IntegerField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='MultiSub',
            fields=[
                ('multibase_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='exampleapp.multibase')),
                ('s1', models.IntegerField(null=True)),
                ('s2', models.IntegerField(null=True)),
            ],
            bases=('exampleapp.multibase',),
        ),
    ]
