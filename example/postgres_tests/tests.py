from django.test import TestCase
from .models import PostgresFields
from psycopg2.extras import NumericRange, DateTimeTZRange, DateRange
from datetime import datetime, date
import pytz


dt = datetime.now(tz=pytz.UTC)


SINGLES = {
    'int1d': [
        None,
        [],
        [None],
        [None, None],
        [None, 123],
        [123, -456, 666]
    ],
    'int2d': [
        None,
        [],
        [None],
        [[None], [None]],
        [[None, None], [123, 456]],
        [[0, -1, None, 123456789]]
    ],
    'txt1d': [
        None,
        [],
        [None],
        [None, None],
        [None, ''],
        ['', 'ümläütß', '{\'"@%%\t\n\\N']
    ],
    'txt2d': [
        None,
        [],
        [None],
        [[None], [None]],
        [[None, None], ['', None]],
        [['{\'"@%%\t\n\\N', '', None, '\\\\\\\\\\}}']]
    ],
    'hstore': [None, {'a': '\\\\\\\\\\', '123': '{\'"@%%\t\n\\N', 'none': None}],
    'int_r': [None, NumericRange(2, 8, '[)'), NumericRange(2, 8), NumericRange(-1, 1, '[]')],
    'dt_r': [None, DateTimeTZRange(dt, datetime(2040, 1, 1))],
    'date_r': [None, DateRange(dt.date(), date(2040, 1, 1))]
}
EXAMPLE = {
    'int1d': [None, -123456, 0, 1],
    'int2d': [[0, None], [1, -1], [None, -666666]],
    'txt1d': [None, '', '\\', '\'', 'ümläütß'],
    'txt2d': [['{\'"@%%\t\n\\N', '', None, '\\\\\\\\\\'], ['"ello"\n', '\x80\x81', '\x01', '']],
    'hstore': {'a': '\\\\\\\\\\', '123': '{\'"@%%\t\n\\N', 'none': None},
    'int_r': NumericRange(-1, 1, '[]'),
    'dt_r': DateTimeTZRange(dt, datetime(2040, 1, 1)),
    'date_r': DateRange(dt.date(), date(2040, 1, 1))
}
FIELDS = tuple(EXAMPLE.keys())


class TestArrayModel(TestCase):
    def test_singles(self):
        for fieldname, values in SINGLES.items():
            for value in values:
                PostgresFields.objects.all().delete()
                a = PostgresFields.objects.create()
                b = PostgresFields.objects.create()
                update_a = PostgresFields(pk=a.pk, **{fieldname: value})
                update_b = PostgresFields(pk=b.pk, **{fieldname: value})
                PostgresFields.objects.bulk_update([update_a], [fieldname])
                PostgresFields.objects.fast_update([update_b], [fieldname])
                res_a, res_b = PostgresFields.objects.all().values(fieldname)
                self.assertEqual(res_b[fieldname], res_a[fieldname])

    def test_updatefull_multiple(self):
        a = []
        b = []
        for _ in range(100):
            a.append(PostgresFields.objects.create())
            b.append(PostgresFields.objects.create())
        update_a = []
        for _a in a:
            update_a.append(PostgresFields(pk=_a.pk, **EXAMPLE))
        update_b = []
        for _b in b:
            update_b.append(PostgresFields(pk=_b.pk, **EXAMPLE))
        PostgresFields.objects.bulk_update(update_a, FIELDS)
        PostgresFields.objects.fast_update(update_b, FIELDS)
        results = list(PostgresFields.objects.all().values(*FIELDS))
        first = results[0]
        for r in results[1:]:
            for f in FIELDS:
                self.assertEqual(r[f], first[f])
