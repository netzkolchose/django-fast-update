from django.test import TestCase
from .models import PostgresFields
from exampleapp.models import FieldUpdate
from psycopg2.extras import NumericRange, DateTimeTZRange, DateRange
import datetime
import pytz
import uuid
from decimal import Decimal


dt = datetime.datetime.now()
dt_utc = datetime.datetime.now(tz=pytz.UTC)
random_uuid = uuid.uuid4()


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
    'dt_r': [None, DateTimeTZRange(dt_utc, datetime.datetime(2040, 1, 1))],
    'date_r': [None, DateRange(dt_utc.date(), datetime.date(2040, 1, 1))]
}
EXAMPLE = {
    'int1d': [None, -123456, 0, 1],
    'int2d': [[0, None], [1, -1], [None, -666666]],
    'txt1d': [None, '', '\\', '\'', 'ümläütß'],
    'txt2d': [['{\'"@%%\t\n\\N', '', None, '\\\\\\\\\\'], ['"ello"\n', '\x80\x81', '\x01', '']],
    'hstore': {'a': '\\\\\\\\\\', '123': '{\'"@%%\t\n\\N', 'none': None},
    'int_r': NumericRange(-1, 1, '[]'),
    'dt_r': DateTimeTZRange(dt_utc, datetime.datetime(2040, 1, 1)),
    'date_r': DateRange(dt_utc.date(), datetime.date(2040, 1, 1))
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


CU_SINGLES = {
    'f_biginteger': [None, -123, 0, 123456789123],
    'f_binary': [None, b'', b'\x00\x00', b'\xff\xff', b'\x80\x81'],
    'f_boolean': [None, True, False],
    'f_char': [None, '', '\t\n"{\'}\\N]', '\t\t', '\\N', '\\N\t\\N'],
    'f_date': [None, dt.date(), datetime.date(1, 1, 1), datetime.date(9999, 12, 31)],
    'f_datetime': [
        None,
        dt_utc,
        pytz.timezone('Europe/Berlin').localize(dt),
        datetime.datetime(1,1,1,1,1,1,1, tzinfo=pytz.UTC),
        datetime.datetime(9999,12,31,23,59,59,999999, tzinfo=pytz.UTC)
    ],
    'f_decimal': [None, Decimal(0), Decimal('2.0'), Decimal('-22.12345')],
    'f_duration': [
        None,
        datetime.timedelta(days=1),
        datetime.timedelta(days=2),
        datetime.timedelta(days=-1),
        datetime.timedelta(days=-2.5),
        dt - datetime.datetime(2010,1,1),
        dt_utc - datetime.datetime(2030,1,1, tzinfo=pytz.UTC),
        datetime.timedelta(days=-1, hours=-1, minutes=-1, seconds=-1, milliseconds=-1),
        datetime.timedelta(seconds=2),
        datetime.timedelta(seconds=-2),
        datetime.timedelta(hours=24)
    ],
    'f_email': [None, 'test@example.com', 'test+folder@example.com'],
    'f_float': [None, None, 0.00000000001, 1.23456789, -1.23456789, 1e-05, 1e25, float('inf'), float('-inf')],
    'f_integer': [None, 0, 1, -1, 123, -123],
    'f_ip': ['127.0.0.1', '2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:db8::1'],
    'f_json': [None, {}, [], [1, None, 3], {'a': None}, {'a': 123.45, 'b': 'umläütß€'}],
    'f_slug': [None, '', 'some random text with ümläutß'],
    'f_smallinteger': [None, 0, 1, -1, 123, -123],
    'f_text': [None, '', 'hello', 'ümläütß€', '1\t2\n3\\n'],
    'f_time': [
        None,
        dt.time(),
        dt_utc.time(),
        datetime.time(hour=0, minute=0, second=0),
        datetime.time(hour=23, minute=59, second=59)
    ],
    'f_uuid': [None, random_uuid]
}

CU_EXAMPLE = {
    'f_biginteger': -123456789123,
    'f_binary': b'\x00\x80\xff',
    'f_boolean': True,
    'f_char': 'with umläütß€',
    'f_date': dt.date(),
    'f_datetime': dt_utc,
    'f_decimal': Decimal(-1.23456),
    'f_duration': datetime.timedelta(days=-1, hours=-1, minutes=-1, seconds=-1, milliseconds=-1),
    'f_email': 'test@example.com',
    'f_float': -1.23456,
    'f_integer': -123456,
    'f_ip': '127.0.0.1',
    'f_json': {'a': 123.45, 'b': 'umläütß€'},
    'f_slug': 'umläütß€',
    'f_smallinteger': -12345,
    'f_text': 'with umläütß€\nand other \t "problematic"',
    'f_time': dt_utc.time(),
    'f_url': 'http://example.com',
    'f_uuid': random_uuid,
}
CU_FIELDS = tuple(CU_EXAMPLE.keys())


class TestCopyUpdate(TestCase):
    def _single(self, fieldname):
        for value in CU_SINGLES[fieldname]:
            FieldUpdate.objects.all().delete()
            a = FieldUpdate.objects.create()
            b = FieldUpdate.objects.create()
            update_a = FieldUpdate(pk=a.pk, **{fieldname: value})
            update_b = FieldUpdate(pk=b.pk, **{fieldname: value})
            FieldUpdate.objects.bulk_update([update_a], [fieldname])
            FieldUpdate.objects.copy_update([update_b], [fieldname])
            res_a, res_b = FieldUpdate.objects.all().values(fieldname)
            self.assertEqual(res_b[fieldname], res_a[fieldname])

    def test_biginteger(self):
        self._single('f_biginteger')

    def test_binary(self):
        self._single('f_binary')

    def test_boolean(self):
        self._single('f_boolean')
    
    def test_char(self):
        self._single('f_char')

    def test_date(self):
        self._single('f_date')

    def test_datetime(self):
        self._single('f_datetime')

    def test_decimal(self):
        self._single('f_decimal')

    def test_duration(self):
        self._single('f_duration')

    def test_email(self):
        self._single('f_email')

    def test_float(self):
        self._single('f_float')

    def test_integer(self):
        self._single('f_integer')

    def test_ip(self):
        self._single('f_ip')

    def test_json(self):
        self._single('f_json')

    def test_slug(self):
        self._single('f_slug')

    def test_text(self):
        self._single('f_text')

    def test_time(self):
        self._single('f_time')

    def test_uuid(self):
        self._single('f_uuid')

    def test_updatefull_multiple(self):
        a = []
        b = []
        for _ in range(100):
            a.append(FieldUpdate.objects.create())
            b.append(FieldUpdate.objects.create())
        update_a = []
        for _a in a:
            update_a.append(FieldUpdate(pk=_a.pk, **CU_EXAMPLE))
        update_b = []
        for _b in b:
            update_b.append(FieldUpdate(pk=_b.pk, **CU_EXAMPLE))
        FieldUpdate.objects.bulk_update(update_a, CU_FIELDS)
        FieldUpdate.objects.copy_update(update_b, CU_FIELDS)
        results = list(FieldUpdate.objects.all().values(*CU_FIELDS))
        first = results[0]
        for r in results[1:]:
            for f in CU_FIELDS:
                self.assertEqual(r[f], first[f])
