from django.core.management.base import BaseCommand
from exampleapp.models import FieldUpdate
from exampleapp.tests import EXAMPLE, FIELDS
from time import time, sleep
from django.db import transaction, connection


def tester(f, n=10):
    runs = []
    for _ in range(n):
        # some sleep to put db at rest
        sleep(1)
        with transaction.atomic():
            FieldUpdate.objects.bulk_create([FieldUpdate() for _ in range(10000)])
            objs = FieldUpdate.objects.all()
            changeset = [FieldUpdate(pk=o.pk, **EXAMPLE) for o in objs]
            start = time()
            f(changeset)
            runs.append(time() - start)
            FieldUpdate.objects.all().delete()
    return sum(runs) / len(runs)


class Command(BaseCommand):
    def handle(self, *args, **options):
        if connection.vendor == 'postgresql':
            print('10 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch[:10], FIELDS)))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch[:10], FIELDS)))
            print('copy_update:', tester(lambda ch : FieldUpdate.objects.copy_update(ch[:10], FIELDS)))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch[:10], FIELDS)))
            print('100 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch[:100], FIELDS)))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch[:100], FIELDS)))
            print('copy_update:', tester(lambda ch : FieldUpdate.objects.copy_update(ch[:100], FIELDS)))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch[:100], FIELDS)))
            print('1000 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch[:1000], FIELDS)))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch[:1000], FIELDS)))
            print('copy_update:', tester(lambda ch : FieldUpdate.objects.copy_update(ch[:1000], FIELDS)))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch[:1000], FIELDS)))
            print('10000 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch, FIELDS), 2))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch, FIELDS), 2))
            print('copy_update:', tester(lambda ch : FieldUpdate.objects.copy_update(ch, FIELDS), 2))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch, FIELDS), 2))

        else:
            print('10 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch[:10], FIELDS)))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch[:10], FIELDS)))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch[:10], FIELDS)))
            print('100 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch[:100], FIELDS)))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch[:100], FIELDS)))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch[:100], FIELDS)))
            print('1000 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch[:1000], FIELDS)))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch[:1000], FIELDS)))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch[:1000], FIELDS)))
            print('10000 instances')
            print('bulk_update:', tester(lambda ch : FieldUpdate.objects.bulk_update(ch, FIELDS), 2))
            print('fast_update:', tester(lambda ch : FieldUpdate.objects.fast_update(ch, FIELDS), 2))
            print('flat_update:', tester(lambda ch : FieldUpdate.objects.flat_update(ch, FIELDS), 2))
