from django.db import connection

import unittest
if connection.vendor != 'postgresql':
    raise unittest.SkipTest('postgres only tests')

from django.test import TestCase
from .models import PostgresFields, FieldUpdateNotNull, CustomField, FieldUpdateArray
from exampleapp.models import FieldUpdate, MultiSub, Child, Parent
from psycopg2.extras import NumericRange, DateTimeTZRange, DateRange
import datetime
import pytz
import uuid
from decimal import Decimal
from fast_update.copy import get_encoder, register_fieldclass, Int, IntOrNone, array_factory
import json


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
    'f_json': [None, {}, [], [1, None, 3], {'a': None}, {'a': 123.45, 'b': 'umläütß€', 'c': '\\N\n{'}],
    'f_slug': [None, '', 'some random text with ümläutß'],
    'f_smallinteger': [None, 0, 1, -1, 123, -123],
    'f_text': [None, '', 'hello', 'ümläütß€', '1\t2\n3\\n', '\\N\'\n{"'],
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
    'f_binary': b'\x00\x80\xff1234567',
    'f_boolean': True,
    'f_char': 'with umläütß€',
    'f_date': dt.date(),
    'f_datetime': dt_utc,
    'f_decimal': Decimal(-1.23456),
    'f_duration': datetime.timedelta(days=-1, hours=-1, minutes=-1, seconds=-1, milliseconds=-1),
    'f_email': 'test@example.com',
    'f_float': -1.23456,
    'f_integer': -123456,
    'f_ip': '1.2.3.4',
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

    def _single_raise(self, fieldname, wrong_value, msg):
        a = FieldUpdate.objects.create()
        setattr(a, fieldname, wrong_value)
        self.assertRaisesMessage(TypeError, msg, lambda : FieldUpdate.objects.copy_update([a], [fieldname]))

    def test_biginteger(self):
        self._single('f_biginteger')
        self._single_raise('f_biginteger', 'wrong', "expected type <class 'int'> or None")


    def test_binary(self):
        self._single('f_binary')
        self._single_raise('f_binary', 'wrong', "expected types <class 'memoryview'>, <class 'bytes'> or None")
    
    def test_binary_big(self):
        # >64k
        data = b'1234567890' * 10000
        obj = FieldUpdate.objects.create()
        obj.f_binary = data
        FieldUpdate.objects.copy_update([obj], ['f_binary'])
        self.assertEqual(FieldUpdate.objects.get(pk=obj.pk).f_binary.tobytes(), data)
        # <64k
        data = b'1234567890' * 1000
        obj = FieldUpdate.objects.create()
        obj.f_binary = data
        FieldUpdate.objects.copy_update([obj], ['f_binary'])
        self.assertEqual(FieldUpdate.objects.get(pk=obj.pk).f_binary.tobytes(), data)

    def test_boolean(self):
        self._single('f_boolean')
        self._single_raise('f_boolean', 'wrong', "expected type <class 'bool'> or None")
    
    def test_char(self):
        self._single('f_char')
        self._single_raise('f_char', 123, "expected type <class 'str'> or None")

    def test_date(self):
        self._single('f_date')
        self._single_raise('f_date', 'wrong', "expected type <class 'datetime.date'> or None")

    def test_datetime(self):
        self._single('f_datetime')
        self._single_raise('f_datetime', 'wrong', "expected type <class 'datetime.datetime'> or None")

    def test_decimal(self):
        self._single('f_decimal')
        self._single_raise('f_decimal', 'wrong', "expected type <class 'decimal.Decimal'> or None")

    def test_duration(self):
        self._single('f_duration')
        self._single_raise('f_duration', 'wrong', "expected type <class 'datetime.timedelta'> or None")

    def test_email(self):
        self._single('f_email')
        self._single_raise('f_email', 123, "expected type <class 'str'> or None")

    def test_float(self):
        self._single('f_float')
        self._single_raise('f_float', 'wrong', "expected types <class 'float'>, <class 'int'> or None")

    def test_integer(self):
        self._single('f_integer')
        self._single_raise('f_integer', 'wrong', "expected type <class 'int'> or None")

    def test_ip(self):
        self._single('f_ip')
        self._single_raise('f_ip', 123, "expected type <class 'str'> or None")

    def test_json(self):
        self._single('f_json')

    def test_slug(self):
        self._single('f_slug')
        self._single_raise('f_slug', 123, "expected type <class 'str'> or None")

    def test_text(self):
        self._single('f_text')
        self._single_raise('f_text', 123, "expected type <class 'str'> or None")

    def test_time(self):
        self._single('f_time')
        self._single_raise('f_time', 'wrong', "expected type <class 'datetime.time'> or None")

    def test_uuid(self):
        self._single('f_uuid')
        self._single_raise('f_uuid', 'wrong', "expected type <class 'uuid.UUID'> or None")

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

    def test_big_lazy(self):
        obj = FieldUpdate.objects.create()
        obj.f_binary = b'0' * 100000
        obj.f_text = 'x' * 100000
        FieldUpdate.objects.copy_update([obj], ['f_binary', 'f_text'])
        self.assertEqual(FieldUpdate.objects.get(pk=obj.pk).f_binary.tobytes(), b'0' * 100000)
        self.assertEqual(FieldUpdate.objects.get(pk=obj.pk).f_text, 'x' * 100000)
    
    def test_lazy_after_big(self):
        obj1 = FieldUpdate.objects.create()
        obj1.f_text = 'x' * 70000
        obj2 = FieldUpdate.objects.create()
        obj2.f_binary = b'0' * 100000
        FieldUpdate.objects.copy_update([obj1, obj2], ['f_binary', 'f_text'])
        self.assertEqual(FieldUpdate.objects.get(pk=obj1.pk).f_text, 'x' * 70000)
        self.assertEqual(FieldUpdate.objects.get(pk=obj2.pk).f_binary.tobytes(), b'0' * 100000)


class TestCopyUpdateNotNull(TestCase):
    def _single(self, fieldname):
        for value in CU_SINGLES[fieldname]:
            if value is None:
                continue
            FieldUpdateNotNull.objects.all().delete()
            a = FieldUpdateNotNull.objects.create()
            b = FieldUpdateNotNull.objects.create()
            update_a = FieldUpdateNotNull(pk=a.pk, **{fieldname: value})
            update_b = FieldUpdateNotNull(pk=b.pk, **{fieldname: value})
            FieldUpdateNotNull.objects.bulk_update([update_a], [fieldname])
            FieldUpdateNotNull.objects.copy_update([update_b], [fieldname])
            res_a, res_b = FieldUpdateNotNull.objects.all().values(fieldname)
            self.assertEqual(res_b[fieldname], res_a[fieldname])

    def _single_raise(self, fieldname, wrong_value, msg):
        a = FieldUpdateNotNull.objects.create()
        setattr(a, fieldname, wrong_value)
        self.assertRaisesMessage(TypeError, msg, lambda : FieldUpdateNotNull.objects.copy_update([a], [fieldname]))

    def test_biginteger(self):
        self._single('f_biginteger')
        self._single_raise('f_biginteger', 'wrong', "expected type <class 'int'>")

    def test_binary(self):
        self._single('f_binary')
        self._single_raise('f_binary', 'wrong', "expected types <class 'memoryview'> or <class 'bytes'>")
    
    def test_binary_big(self):
        # >64k
        data = b'1234567890' * 10000
        obj = FieldUpdateNotNull.objects.create()
        obj.f_binary = data
        FieldUpdateNotNull.objects.copy_update([obj], ['f_binary'])
        self.assertEqual(FieldUpdateNotNull.objects.get(pk=obj.pk).f_binary.tobytes(), data)
        # <64k
        data = b'1234567890' * 1000
        obj = FieldUpdateNotNull.objects.create()
        obj.f_binary = data
        FieldUpdateNotNull.objects.copy_update([obj], ['f_binary'])
        self.assertEqual(FieldUpdateNotNull.objects.get(pk=obj.pk).f_binary.tobytes(), data)

    def test_boolean(self):
        self._single('f_boolean')
        self._single_raise('f_boolean', 'wrong', "expected type <class 'bool'>")
    
    def test_char(self):
        self._single('f_char')
        self._single_raise('f_char', 123, "expected type <class 'str'>")

    def test_date(self):
        self._single('f_date')
        self._single_raise('f_date', 'wrong', "expected type <class 'datetime.date'>")

    def test_datetime(self):
        self._single('f_datetime')
        self._single_raise('f_datetime', 'wrong', "expected type <class 'datetime.datetime'>")

    def test_decimal(self):
        self._single('f_decimal')
        self._single_raise('f_decimal', 'wrong', "expected type <class 'decimal.Decimal'>")

    def test_duration(self):
        self._single('f_duration')
        self._single_raise('f_duration', 'wrong', "expected type <class 'datetime.timedelta'>")

    def test_email(self):
        self._single('f_email')
        self._single_raise('f_email', 123, "expected type <class 'str'>")

    def test_float(self):
        self._single('f_float')
        self._single_raise('f_float', 'wrong', "expected types <class 'float'> or <class 'int'>")

    def test_integer(self):
        self._single('f_integer')
        self._single_raise('f_integer', 'wrong', "expected type <class 'int'>")

    def test_ip(self):
        self._single('f_ip')
        self._single_raise('f_ip', 123, "expected type <class 'str'>")

    def test_json(self):
        self._single('f_json')

    def test_slug(self):
        self._single('f_slug')
        self._single_raise('f_slug', 123, "expected type <class 'str'>")

    def test_text(self):
        self._single('f_text')
        self._single_raise('f_text', 123, "expected type <class 'str'>")

    def test_time(self):
        self._single('f_time')
        self._single_raise('f_time', 'wrong', "expected type <class 'datetime.time'>")

    def test_uuid(self):
        self._single('f_uuid')
        self._single_raise('f_uuid', 'wrong', "expected type <class 'uuid.UUID'>")


    def test_updatefull_multiple(self):
        a = []
        b = []
        for _ in range(100):
            a.append(FieldUpdateNotNull.objects.create())
            b.append(FieldUpdateNotNull.objects.create())
        update_a = []
        for _a in a:
            update_a.append(FieldUpdateNotNull(pk=_a.pk, **CU_EXAMPLE))
        update_b = []
        for _b in b:
            update_b.append(FieldUpdateNotNull(pk=_b.pk, **CU_EXAMPLE))
        FieldUpdateNotNull.objects.bulk_update(update_a, CU_FIELDS)
        FieldUpdateNotNull.objects.copy_update(update_b, CU_FIELDS)
        results = list(FieldUpdateNotNull.objects.all().values(*CU_FIELDS))
        first = results[0]
        for r in results[1:]:
            for f in CU_FIELDS:
                self.assertEqual(r[f], first[f])
        # force threaded write
        update_c = update_b * 100
        FieldUpdateNotNull.objects.copy_update(update_c, CU_FIELDS)
        results = list(FieldUpdateNotNull.objects.all().values(*CU_FIELDS))
        for r in results[201:]:
            for f in CU_FIELDS:
                self.assertEqual(r[f], first[f])


class TestForeignkeyField(TestCase):
    def test_move_parents(self):
        childA = Child.objects.create()
        childB = Child.objects.create()
        parents = [Parent.objects.create(child=childA) for _ in range(10)]
        # move objs
        for parent in parents:
            parent.child = childB
        Parent.objects.copy_update(parents, fields=['child'])
        for obj in Parent.objects.all():
            self.assertEqual(obj.child, childB)

    def test_django_bug_33322(self):
        # https://code.djangoproject.com/ticket/33322
        parent = Parent.objects.create(child=None)
        parent.child = Child()
        parent.child.save()
        Parent.objects.copy_update([parent], fields=['child'])
        self.assertEqual(parent.child_id, parent.child.pk)
        self.assertEqual(Parent.objects.get(pk=parent.pk).child_id, parent.child.pk)


class TestNonlocalFields(TestCase):
    def test_local_nonlocal_mixed(self):
        objs = [MultiSub.objects.create() for _ in range(10)]
        for i, obj in enumerate(objs):
            obj.b1 = i
            obj.b2 = i * 10
            obj.s1 = i * 100
            obj.s2 = i * 1000
        MultiSub.objects.copy_update(objs, ['b1', 'b2', 's1', 's2'])
        self.assertEqual(
            list(MultiSub.objects.all().values_list('b1', 'b2', 's1', 's2').order_by('pk')),
            [(i, i*10, i*100, i*1000) for i in range(10)]
        )
    
    def test_nonlocal_only(self):
        objs = [MultiSub.objects.create() for _ in range(10)]
        for i, obj in enumerate(objs):
            obj.b1 = i
            obj.b2 = i * 10
        MultiSub.objects.copy_update(objs, ['b1', 'b2'])
        self.assertEqual(
            list(MultiSub.objects.all().values_list('b1', 'b2', 's1', 's2').order_by('pk')),
            [(i, i*10, None, None) for i in range(10)]
        )

    def test_local_only(self):
        objs = [MultiSub.objects.create() for _ in range(10)]
        for i, obj in enumerate(objs):
            obj.s1 = i * 100
            obj.s2 = i * 1000
        MultiSub.objects.copy_update(objs, ['s1', 's2'])
        self.assertEqual(
            list(MultiSub.objects.all().values_list('b1', 'b2', 's1', 's2').order_by('pk')),
            [(None, None, i*100, i*1000) for i in range(10)]
        )


class TestCopyUpdateHStore(TestCase):
    def test_hstore(self):
        # test hstore explicit for likely to clash values
        value = {
            'a': None,
            'b"': '\tcomplicated "with quotes"',
            'c': '\\N\n\\n{',
            '': 'empty key',
            'different quotes': '" \\" \\\\"',
            '€': 'ümläütß'
        }
        obj1 = PostgresFields.objects.create()
        obj1.hstore = value
        PostgresFields.objects.bulk_update([obj1], ['hstore'])
        obj2 = PostgresFields.objects.create()
        obj2.hstore = value
        PostgresFields.objects.copy_update([obj2], ['hstore'])
        self.assertEqual(
            PostgresFields.objects.get(pk=obj1.pk).hstore,
            value
        )
        self.assertEqual(
            PostgresFields.objects.get(pk=obj2.pk).hstore,
            PostgresFields.objects.get(pk=obj1.pk).hstore
        )
        # top level null
        obj2.hstore = None
        PostgresFields.objects.copy_update([obj2], ['hstore'])
        self.assertEqual(
            PostgresFields.objects.get(pk=obj2.pk).hstore,
            None
        )

    def test_hstore_wrongtype(self):
        obj = PostgresFields.objects.create()
        obj.hstore = 123
        self.assertRaisesMessage(
            TypeError,
            "expected type <class 'dict'> or None",
            lambda : PostgresFields.objects.copy_update([obj], ['hstore'])
        )

    def test_hstore_wrongkeytype(self):
        obj = PostgresFields.objects.create()
        obj.hstore = {123: 'nothing'}
        self.assertRaisesMessage(
            TypeError,
            "expected type <class 'str'> for keys",
            lambda : PostgresFields.objects.copy_update([obj], ['hstore'])
        )

    def test_hstore_wrongvaluetype(self):
        obj = PostgresFields.objects.create()
        obj.hstore = {'wrong': 123}
        self.assertRaisesMessage(
            TypeError,
            "expected type <class 'str'> or None for values",
            lambda : PostgresFields.objects.copy_update([obj], ['hstore'])
        )


class TestCopyUpdateRangeFields(TestCase):
    def _singles(self, fieldname, value):
        obj1 = PostgresFields.objects.create()
        setattr(obj1, fieldname, value)
        PostgresFields.objects.bulk_update([obj1], [fieldname])
        obj2 = PostgresFields.objects.create()
        setattr(obj2, fieldname, value)
        PostgresFields.objects.copy_update([obj2], [fieldname])
        self.assertEqual(
            getattr(PostgresFields.objects.get(pk=obj2.pk), fieldname),
            getattr(PostgresFields.objects.get(pk=obj1.pk), fieldname)
        )

    def _single_raise(self, fieldname, wrong_value, msg):
        a = PostgresFields.objects.create()
        setattr(a, fieldname, wrong_value)
        self.assertRaisesMessage(TypeError, msg, lambda : PostgresFields.objects.copy_update([a], [fieldname]))

    def test_integer_range(self):
        values = [None, NumericRange(2, 8, '[)'), NumericRange(2, 8), NumericRange(-1, 1, '[]')]
        for v in values:
            self._singles('int_r', v)
        self._single_raise('int_r', NumericRange('[[[', 8, '[)'), "expected type <class 'int'> or None")

    def test_datetime_range(self):
        values = [None, DateTimeTZRange(dt_utc, datetime.datetime(2040, 1, 1))]
        for v in values:
            self._singles('dt_r', v)
        self._single_raise('dt_r', DateTimeTZRange('[[[', 8, '[)'), "expected type <class 'datetime.datetime'> or None")

    def test_date_range(self):
        values = [None, DateRange(dt_utc.date(), datetime.date(2040, 1, 1))]
        for v in values:
            self._singles('date_r', v)
        self._single_raise('date_r', DateRange('[[[', 8, '[)'), "expected type <class 'datetime.date'> or None")


ARRAY_SINGLES = {
    'f_biginteger': [
        None,
        [],
        [None],
        [None, None],
        [None, 123456789123],
        [123, -456, 66666666666]
    ],
    'f_binary': [
        None,
        [],
        [None],
        [None, None],
        [b''],
        [b'', b'', None],
        [b'123', b'\x80\x80', memoryview(b'666')],
    ],
    'f_boolean': [
        None,
        [],
        [None],
        [None, None],
        [True],
        [None, True, False]
    ],
    'f_char': [
        None,
        [],
        [None],
        [None, None],
        [''],
        ['', '', None],
        [None, '', '\t\n"{\'}]', '{{', '\\N', '\t\n"', '\'', 'ümläütß', '" \\" \\\\"']
    ],
    'f_date': [
        None,
        [],
        [None],
        [None, None],
        [None, dt.date(), datetime.date(1, 1, 1), datetime.date(9999, 12, 31)]
    ],
    'f_datetime': [
        None,
        [],
        [None],
        [None, None],
        [
            None,
            dt_utc,
            pytz.timezone('Europe/Berlin').localize(dt),
            datetime.datetime(1,1,1,1,1,1,1, tzinfo=pytz.UTC),
            datetime.datetime(9999,12,31,23,59,59,999999, tzinfo=pytz.UTC)
        ]
    ],
    'f_decimal': [
        None,
        [],
        [None],
        [None, None],
        [Decimal(0)],
        [Decimal('2.0'), None],
        [Decimal('2.0'), Decimal('-22.12345'), Decimal(0), None]
    ],
    'f_duration': [
        None,
        [],
        [None],
        [None, None],
        [
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
        ]
    ],
    'f_email': [
        None,
        [],
        [None],
        [None, None],
        [None, '', 'test@example.com', 'test+folder@example.com']
    ],
    'f_float': [
        None,
        [],
        [None],
        [None, None],
        [None, None, 0.00000000001, 1.23456789, -1.23456789, 1e-05, 1e25],
        [float('-inf'), None]
    ],
    'f_integer': [
        None,
        [],
        [None],
        [None, None],
        [None, 123456789],
        [123, -456, 666666666]
    ],
    'f_ip': [
        None,
        [],
        [None],
        [None, None],
        ['127.0.0.1', '2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:db8::1', None]
    ],
    'f_json': [
        None,
        [],
        [None],
        [None, None],
        [{
            'a': None,
            'b"': '\tcomplicated "with quotes"',
            'c': '\\N\n\\n{',
            '': 'empty key',
            'different quotes': '" \\" \\\\"',
            '€': 'ümläütß'
        }, None, 'plain string', (1,2,'\t\t')]
    ],
    'f_slug': [
        None,
        [],
        [None],
        [None, None],
        [None, '', 'some random text with ümläutß']
    ],
    'f_smallinteger': [
        None,
        [],
        [None],
        [None, None],
        [None, 0, 1, -1, 123, -123]
    ],
    'f_text': [
        None,
        [],
        [None],
        [None, None],
        [None, '', 'hello', 'ümläütß€', '1\t2\n3\\n', '" \\" \\\\"', '\\N\n\\n{'],
        [None, '', '\\N', '"{', '{hel,lo}', 'a\\b\'c', '\t', '\n', 'mo,u{s"e']
    ],
    'f_time': [
        None,
        [],
        [None],
        [None, None],
        [
            None,
            dt.time(),
            dt_utc.time(),
            datetime.time(hour=0, minute=0, second=0),
            datetime.time(hour=23, minute=59, second=59)
        ]
    ],
    'f_url': [
        None,
        [],
        [None],
        [None, None],
        [None, '', 'http://example.com', 'https://!$%25:)(*&^@www.example.com/blog/urls.html?abc=123#what-else']
    ],
    'f_uuid': [
        None,
        [],
        [None],
        [None, None],
        [None, random_uuid, random_uuid]
    ],
    'f_biginteger2': [],
    'f_binary2': [],
    'f_boolean2': [],
    'f_char2': [],
    'f_date2': [],
    'f_datetime2': [],
    'f_decimal2': [],
    'f_duration2': [],
    'f_email2': [],
    'f_float2': [],
    'f_integer2': [],
    'f_ip2': [],
    'f_json2': [],
    'f_slug2': [],
    'f_smallinteger2': [],
    'f_text2': [],
    'f_time2': [],
    'f_url2': [],
    'f_uuid2': [],
}


class TestCopyUpdateArray(TestCase):
    # TODO: test float NaN for arrays
    # TODO: 2d tests
    def test_singles(self):
        for fieldname, values in ARRAY_SINGLES.items():
            for value in values:
                FieldUpdateArray.objects.all().delete()
                a = FieldUpdateArray.objects.create()
                b = FieldUpdateArray.objects.create()
                update_a = FieldUpdateArray(pk=a.pk, **{fieldname: value})
                update_b = FieldUpdateArray(pk=b.pk, **{fieldname: value})
                FieldUpdateArray.objects.bulk_update([update_a], [fieldname])
                FieldUpdateArray.objects.copy_update([update_b], [fieldname])
                res_a, res_b = FieldUpdateArray.objects.all().values(fieldname)
                self.assertEqual(res_b[fieldname], res_a[fieldname])


class TestCopyUpdateWickedText(TestCase):
    def test_plain_bytes(self):
        # skip \x00 as it is not allowed in postgres
        values = ''.join(chr(i) for i in range(1, 256))
        obj = FieldUpdate.objects.create()
        # full write
        obj.f_text = values
        FieldUpdate.objects.copy_update([obj], ['f_text'])
        self.assertEqual(FieldUpdate.objects.get(pk=obj.pk).f_text, values)
        # single writes
        for v in values:
            obj.f_text = v
            FieldUpdate.objects.copy_update([obj], ['f_text'])
            self.assertEqual(FieldUpdate.objects.get(pk=obj.pk).f_text, v)
        # array write singles
        obj_ar = FieldUpdateArray.objects.create()
        obj_ar.f_text = list(values)
        FieldUpdateArray.objects.copy_update([obj_ar], ['f_text'])
        self.assertEqual(FieldUpdateArray.objects.get(pk=obj_ar.pk).f_text, list(values))
        # array write block
        obj_ar.f_text = [values, values]
        FieldUpdateArray.objects.copy_update([obj_ar], ['f_text'])
        self.assertEqual(FieldUpdateArray.objects.get(pk=obj_ar.pk).f_text, [values, values])
        # array 2d
        obj_ar.f_text2 = [list(values), [values] + [None]*254]
        FieldUpdateArray.objects.copy_update([obj_ar], ['f_text2'])
        self.assertEqual(FieldUpdateArray.objects.get(pk=obj_ar.pk).f_text2, [list(values), [values] + [None]*254])

    def test_blns(self):
        # taken from https://github.com/minimaxir/big-list-of-naughty-strings
        from django.conf import settings
        from os import path
        with open(path.join(settings.BASE_DIR, 'blns.json')) as f:
            strings = json.load(f)
            for s in strings:
                a = FieldUpdate.objects.create()
                b = FieldUpdate.objects.create()
                a.f_text = s
                b.f_text = s
                FieldUpdate.objects.bulk_update([a], ['f_text'])
                FieldUpdate.objects.copy_update([b], ['f_text'])
                self.assertEqual(
                    FieldUpdate.objects.get(pk=b.pk).f_text,
                    FieldUpdate.objects.get(pk=a.pk).f_text
                )
            # array write
            a_ar = FieldUpdateArray.objects.create()
            b_ar = FieldUpdateArray.objects.create()
            a_ar.f_text = strings
            b_ar.f_text = strings
            FieldUpdateArray.objects.bulk_update([a_ar], ['f_text'])
            FieldUpdateArray.objects.copy_update([b_ar], ['f_text'])
            self.assertEqual(
                FieldUpdateArray.objects.get(pk=b_ar.pk).f_text,
                FieldUpdateArray.objects.get(pk=a_ar.pk).f_text
            )


class TestFieldRegistration(TestCase):
    def test_register(self):
        f = CustomField()
        f_null = CustomField(null=True)
        self.assertRaisesMessage(
            NotImplementedError,
            'no suitable encoder found for field <postgres_tests.models.CustomField>',
            lambda : get_encoder(f)
        )
        register_fieldclass(CustomField, Int, IntOrNone)
        self.assertEqual(get_encoder(f), Int)
        self.assertEqual(get_encoder(f_null), IntOrNone)


class TestEncoderOverrides(TestCase):
    @property
    def base_encoder(self):
        # some custom encoder pulling floats from german comma str repr
        def customEnc(v, fname, lazy):
            return float(v.replace(',', '.'))
        customEnc.array_escape = False
        return customEnc

    def test_override(self):
        objA = FieldUpdate.objects.create()
        objB = FieldUpdate.objects.create()
        objA.f_float = '1,2345'
        objB.f_float = '-0,00001'

        # write to db with custom encoder
        FieldUpdate.objects.copy_update(
            [objA, objB], ['f_float'],
            field_encoders={'f_float': self.base_encoder}
        )
        self.assertEqual(FieldUpdate.objects.get(pk=objA.pk).f_float, 1.2345)
        self.assertEqual(FieldUpdate.objects.get(pk=objB.pk).f_float, -0.00001)

    def test_override_array(self):
        objA = FieldUpdateArray.objects.create()
        objB = FieldUpdateArray.objects.create()
        objA.f_float = ['1,2345', '666']
        objB.f_float = ['-0,00001', 'inf']

        # write to db with custom encoder
        FieldUpdateArray.objects.copy_update(
            [objA, objB], ['f_float'],
            field_encoders={'f_float': array_factory(self.base_encoder, 1, False)}
        )
        self.assertEqual(FieldUpdateArray.objects.get(pk=objA.pk).f_float, [1.2345, 666])
        self.assertEqual(FieldUpdateArray.objects.get(pk=objB.pk).f_float, [-0.00001, float('inf')])


class TestArrayEvaluation(TestCase):
    def test_empty_reduction(self):
        a = FieldUpdateArray.objects.create()
        b = FieldUpdateArray.objects.create()

        # these all eval to empty array '{}' in ARRAY[] notation
        values = [
            [],
            [[], None],
            [[], [], None],
        ]
        for v in values:
            a.f_integer2 = v
            b.f_integer2 = v
            FieldUpdateArray.objects.bulk_update([a], ['f_integer2'])
            FieldUpdateArray.objects.copy_update([b], ['f_integer2'])
            self.assertEqual(FieldUpdateArray.objects.get(pk=a.pk).f_integer2, [])
            self.assertEqual(FieldUpdateArray.objects.get(pk=b.pk).f_integer2, [])

    def test_unbalanced1(self):
        a = FieldUpdateArray.objects.create()
        a.f_integer2 = [[1,2], [3]]
        self.assertRaisesMessage(
            ValueError,
            'multidimensional arrays must be balanced',
            lambda: FieldUpdateArray.objects.copy_update([a], ['f_integer2'])
        )

    def test_unbalanced2(self):
        a = FieldUpdateArray.objects.create()
        a.f_integer2 = [1, 2, [3]]
        self.assertRaisesMessage(
            ValueError,
            'multidimensional arrays must be balanced',
            lambda: FieldUpdateArray.objects.copy_update([a], ['f_integer2'])
        )

    def test_unbalanced3(self):
        a = FieldUpdateArray.objects.create()
        a.f_integer2 = [None, [None]]
        self.assertRaisesMessage(
            ValueError,
            'multidimensional arrays must be balanced',
            lambda: FieldUpdateArray.objects.copy_update([a], ['f_integer2'])
        )



# TODO: test hstore/range types with array...
# TODO: document encoder interface
