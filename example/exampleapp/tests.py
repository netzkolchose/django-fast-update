import datetime
import pytz
from decimal import Decimal
import uuid
from math import isnan
from unittest import skipUnless
from django.test import TestCase
from parameterized import parameterized
from django.db import connection
from django.db.models import F
from django.test.utils import CaptureQueriesContext
from .models import FieldUpdate, MultiBase, Parent, Child, MultiSub

dt = datetime.datetime.now()
dt_utc = datetime.datetime.now(tz=pytz.UTC)
random_uuid = uuid.uuid4()


# single value tests
SINGLES = {
    'f_biginteger': [None, -123, 0, 123456789123],
    'f_binary': [None, b'', b'\x00\x00', b'\xff\xff', b'\x80\x81'],
    'f_boolean': [None, True, False],
    'f_char': [None, '', '\t\n"{\'}]'],
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
    'f_float': [None, None, 0.00000000001, 1.23456789, -1.23456789, 1e-05, 1e25],
    'f_integer': [None, 0, 1, -1, 123, -123],
    'f_ip': ['127.0.0.1', '2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:db8::1'],
    'f_json': [None, {}, [], [1, None, 3], {'a': None}],
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


# full dataset for model FieldUpdate
EXAMPLE = {
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
FIELDS = tuple(EXAMPLE.keys())


UPDATE_IMPLS = [
    ['fast_update'],
    ['flat_update'],
    ['merged_update'],
]


class TestLocalFields(TestCase):
    @parameterized.expand(UPDATE_IMPLS)
    def test_singles(self, update_impl):
        for fieldname, values in SINGLES.items():
            for value in values:
                FieldUpdate.objects.all().delete()
                a = FieldUpdate.objects.create()
                b = FieldUpdate.objects.create()
                update_a = FieldUpdate(pk=a.pk, **{fieldname: value})
                update_b = FieldUpdate(pk=b.pk, **{fieldname: value})
                FieldUpdate.objects.bulk_update([update_a], [fieldname])
                getattr(FieldUpdate.objects, update_impl)([update_b], [fieldname])
                res_a, res_b = FieldUpdate.objects.all().values(fieldname)
                self.assertEqual(res_b[fieldname], res_a[fieldname])

    @parameterized.expand(UPDATE_IMPLS)
    @skipUnless(connection.vendor == 'postgresql', 'postgres only tests')
    def test_inf(self, update_impl):
        a = FieldUpdate.objects.create()
        b = FieldUpdate.objects.create()
        update_a = FieldUpdate(pk=a.pk, f_float=float('inf'))
        update_b = FieldUpdate(pk=b.pk, f_float=float('inf'))
        FieldUpdate.objects.bulk_update([update_a], ['f_float'])
        getattr(FieldUpdate.objects, update_impl)([update_b], ['f_float'])
        res_a, res_b = FieldUpdate.objects.all().values('f_float')
        self.assertEqual(res_b['f_float'], res_a['f_float'])
    
    @parameterized.expand(UPDATE_IMPLS)
    @skipUnless(connection.vendor == 'postgresql', 'postgres only tests')
    def test_nan(self, update_impl):
        a = FieldUpdate.objects.create()
        b = FieldUpdate.objects.create()
        update_a = FieldUpdate(pk=a.pk, f_float=float('NaN'))
        update_b = FieldUpdate(pk=b.pk, f_float=float('NaN'))
        FieldUpdate.objects.bulk_update([update_a], ['f_float'])
        getattr(FieldUpdate.objects, update_impl)([update_b], ['f_float'])
        res_a, res_b = FieldUpdate.objects.all().values('f_float')
        self.assertEqual(isnan(res_a['f_float']), True)
        self.assertEqual(isnan(res_b['f_float']), True)

    @parameterized.expand(UPDATE_IMPLS)
    def test_updatefull(self, update_impl):
        a = FieldUpdate.objects.create()
        b = FieldUpdate.objects.create()
        update_a = FieldUpdate(pk=a.pk, **EXAMPLE)
        update_b = FieldUpdate(pk=b.pk, **EXAMPLE)
        FieldUpdate.objects.bulk_update([update_a], FIELDS)
        getattr(FieldUpdate.objects, update_impl)([update_b], FIELDS)
        res_a, res_b = FieldUpdate.objects.all().values(*FIELDS)
        for f in FIELDS:
            self.assertEqual(res_b[f], res_a[f])

    @parameterized.expand(UPDATE_IMPLS)
    def test_updatefull_multiple(self, update_impl):
        a = []
        b = []
        for _ in range(100):
            a.append(FieldUpdate.objects.create())
            b.append(FieldUpdate.objects.create())
        update_a = []
        for _a in a:
            update_a.append(FieldUpdate(pk=_a.pk, **EXAMPLE))
        update_b = []
        for _b in b:
            update_b.append(FieldUpdate(pk=_b.pk, **EXAMPLE))
        FieldUpdate.objects.bulk_update(update_a, FIELDS)
        getattr(FieldUpdate.objects, update_impl)(update_b, FIELDS)
        results = list(FieldUpdate.objects.all().values(*FIELDS))
        first = results[0]
        for r in results[1:]:
            for f in FIELDS:
                self.assertEqual(r[f], first[f])


@skipUnless(connection.vendor == 'mysql', 'mysql only tests')
class TestPlaceholderCall(TestCase):
    @parameterized.expand(UPDATE_IMPLS)
    def test_mysql_binary(self, update_impl):
        # currently only BinaryField uses a custom placeholder for mysql as '_binary %s',
        # thus we scan for '_binary ' in the sent sql
        b = FieldUpdate.objects.create()
        update_b = FieldUpdate(pk=b.pk, **EXAMPLE)
        with CaptureQueriesContext(connection) as capture:
            getattr(FieldUpdate.objects, update_impl)([update_b], FIELDS)
        queries = capture.captured_queries
        self.assertEqual(len(queries), 1)
        sql = queries[0]['sql']
        # '_binary ' should be exactly once in the statement
        self.assertEqual(sql.count('_binary '), 1)


class TestForeignkeyField(TestCase):
    @parameterized.expand(UPDATE_IMPLS)
    def test_move_parents(self, update_impl):
        childA = Child.objects.create()
        childB = Child.objects.create()
        parents = [Parent.objects.create(child=childA) for _ in range(10)]
        # move objs
        for parent in parents:
            parent.child = childB
        getattr(Parent.objects, update_impl)(parents, fields=['child'])
        for obj in Parent.objects.all():
            self.assertEqual(obj.child, childB)

    @parameterized.expand(UPDATE_IMPLS)
    def test_django_bug_33322(self, update_impl):
        # https://code.djangoproject.com/ticket/33322
        parent = Parent.objects.create(child=None)
        parent.child = Child()
        parent.child.save()
        getattr(Parent.objects, update_impl)([parent], fields=['child'])
        self.assertEqual(parent.child_id, parent.child.pk)
        self.assertEqual(Parent.objects.get(pk=parent.pk).child_id, parent.child.pk)


class TestRaiseOnExpressions(TestCase):
    @parameterized.expand(UPDATE_IMPLS)
    def test_expressions(self, update_impl):
        values = [
            F('f_biginteger'),
            F('f_biginteger') + 1,
            F('f_biginteger') - F('f_smallinteger'),
        ]
        obj = FieldUpdate.objects.create(f_integer=123, f_biginteger=456, f_smallinteger=789)
        for value in values:
            setattr(obj, 'f_integer', value)
            self.assertRaisesMessage(
                ValueError,
                update_impl + '() cannot be used with f-expressions.',
                lambda : getattr(FieldUpdate.objects, update_impl)([obj], ['f_integer'])
            )
        self.assertEqual(
            FieldUpdate.objects.all().values_list('f_integer', 'f_biginteger', 'f_smallinteger')[0],
            (123, 456, 789)
        )
        FieldUpdate.objects.bulk_update([obj], ['f_integer'])
        self.assertEqual(
            FieldUpdate.objects.all().values_list('f_integer', 'f_biginteger', 'f_smallinteger')[0],
            (456-789, 456, 789)
        )


class TestNonlocalFields(TestCase):
    @parameterized.expand(UPDATE_IMPLS)
    def test_local_nonlocal_mixed(self, update_impl):
        objs = [MultiSub.objects.create() for _ in range(10)]
        for i, obj in enumerate(objs):
            obj.b1 = i
            obj.b2 = i * 10
            obj.s1 = i * 100
            obj.s2 = i * 1000
        getattr(MultiSub.objects, update_impl)(objs, ['b1', 'b2', 's1', 's2'])
        self.assertEqual(
            list(MultiSub.objects.all().values_list('b1', 'b2', 's1', 's2').order_by('pk')),
            [(i, i*10, i*100, i*1000) for i in range(10)]
        )

    @parameterized.expand(UPDATE_IMPLS)
    def test_nonlocal_only(self, update_impl):
        objs = [MultiSub.objects.create() for _ in range(10)]
        for i, obj in enumerate(objs):
            obj.b1 = i
            obj.b2 = i * 10
        getattr(MultiSub.objects, update_impl)(objs, ['b1', 'b2'])
        self.assertEqual(
            list(MultiSub.objects.all().values_list('b1', 'b2', 's1', 's2').order_by('pk')),
            [(i, i*10, None, None) for i in range(10)]
        )

    @parameterized.expand(UPDATE_IMPLS)
    def test_local_only(self, update_impl):
        objs = [MultiSub.objects.create() for _ in range(10)]
        for i, obj in enumerate(objs):
            obj.s1 = i * 100
            obj.s2 = i * 1000
        getattr(MultiSub.objects, update_impl)(objs, ['s1', 's2'])
        self.assertEqual(
            list(MultiSub.objects.all().values_list('b1', 'b2', 's1', 's2').order_by('pk')),
            [(None, None, i*100, i*1000) for i in range(10)]
        )


class TestSanityChecks(TestCase):
    def setUp(self):
        self.instances = [
            FieldUpdate.objects.create(**EXAMPLE),
            FieldUpdate.objects.create(**EXAMPLE)
        ]

    @parameterized.expand(UPDATE_IMPLS)
    def test_sanity_checks(self, update_impl):
        # negative batch_size
        self.assertRaisesMessage(
            ValueError,
            'Batch size must be a positive integer.',
            lambda : getattr(FieldUpdate.objects, update_impl)(self.instances, ['f_char'], batch_size=-10)
        )
        # no fieldnames
        self.assertRaisesMessage(
            ValueError,
            f'Field names must be given to {update_impl}().',
            lambda : getattr(FieldUpdate.objects, update_impl)(self.instances, [])
        )
        self.assertRaisesMessage(
            ValueError,
            f'Field names must be given to {update_impl}().',
            lambda : getattr(FieldUpdate.objects, update_impl)(self.instances, fields=None)
        )
        # no objs
        self.assertEqual(getattr(FieldUpdate.objects, update_impl)([], ['f_char']), 0)
        # objs with no pk
        self.assertRaisesMessage(
            ValueError,
            f'All {update_impl}() objects must have a primary key set.',
            lambda : getattr(FieldUpdate.objects, update_impl)([FieldUpdate(**EXAMPLE)], ['f_char'])
        )
        # non concrete field
        mbase = MultiBase.objects.create()
        self.assertRaisesMessage(
            ValueError,
            f'{update_impl}() can only be used with concrete fields.',
            lambda : getattr(MultiBase.objects, update_impl)([mbase], ['multisub'])
        )
        # pk in fields
        self.assertRaisesMessage(
            ValueError,
            f'{update_impl}() cannot be used with primary key fields.',
            lambda : getattr(FieldUpdate.objects, update_impl)(self.instances, ['f_char', 'id'])
        )


class TestDuplicates(TestCase):
    @parameterized.expand(UPDATE_IMPLS)
    def test_raise_on_duplicates(self, update_impl):
        a = FieldUpdate.objects.create()
        with self.assertRaisesMessage(ValueError, 'cannot update duplicates'):
            getattr(FieldUpdate.objects, update_impl)([
                FieldUpdate(pk=a.pk),
                FieldUpdate(pk=a.pk)
            ], FIELDS)

    @parameterized.expand(UPDATE_IMPLS)
    def test_no_pk_duplicates(self, update_impl):
        with self.assertRaisesMessage(ValueError, 'cannot update duplicates'):
            getattr(FieldUpdate.objects, update_impl)([
                FieldUpdate(),
                FieldUpdate()
            ], FIELDS)


class TestPrefiltering(TestCase):
    @parameterized.expand(UPDATE_IMPLS)
    def test_filter_sametable(self, update_impl):
        a = MultiSub.objects.create(s1=1)
        b = MultiSub.objects.create(s1=10)
        c = MultiSub.objects.create(s1=100)
        objs = [a, b, c]

        # filtering
        for i, o in enumerate(objs):
            o.s2 = i + 10
            o.b2 = i + 10
        getattr(MultiSub.objects.filter(s1__gt=9), update_impl)(objs, ['s2', 'b2'])
        self.assertEqual(
            list(MultiSub.objects.all().order_by('pk').values_list('s2', 'b2')),
            [(None, None), (11, 11), (12, 12)]
        )

        # excluding
        for i, o in enumerate(objs):
            o.s2 = (i + 5) * 100
            o.b2 = (i + 5) * 100
        getattr(MultiSub.objects.exclude(s1=10), update_impl)(objs, ['s2', 'b2'])
        self.assertEqual(
            list(MultiSub.objects.all().order_by('pk').values_list('s2', 'b2')),
            [(500, 500), (11, 11), (700, 700)]
        )

        # unfiltered should apply to all
        for i, o in enumerate(objs):
            o.s2 = -i
            o.b2 = -i
        getattr(MultiSub.objects.filter(s1__gt=9), update_impl)(objs, ['s2', 'b2'], unfiltered=True)
        self.assertEqual(
            list(MultiSub.objects.all().order_by('pk').values_list('s2', 'b2')),
            [(0, 0), (-1, -1), (-2, -2)]
        )

    @parameterized.expand(UPDATE_IMPLS)
    def test_filter_mt_othertable(self, update_impl):
        a = MultiSub.objects.create(b1=1)
        b = MultiSub.objects.create(b1=10)
        c = MultiSub.objects.create(b1=100)
        objs = [a, b, c]

        # filtering
        for i, o in enumerate(objs):
            o.s2 = i + 10
            o.b2 = i + 10
        getattr(MultiSub.objects.filter(b1__gt=9), update_impl)(objs, ['s2', 'b2'])
        self.assertEqual(
            list(MultiSub.objects.all().order_by('pk').values_list('s2', 'b2')),
            [(None, None), (11, 11), (12, 12)]
        )

        # excluding
        for i, o in enumerate(objs):
            o.s2 = (i + 5) * 100
            o.b2 = (i + 5) * 100
        getattr(MultiSub.objects.exclude(b1=10), update_impl)(objs, ['s2', 'b2'])
        self.assertEqual(
            list(MultiSub.objects.all().order_by('pk').values_list('s2', 'b2')),
            [(500, 500), (11, 11), (700, 700)]
        )

        # unfiltered should apply to all
        for i, o in enumerate(objs):
            o.s2 = -i
            o.b2 = -i
        getattr(MultiSub.objects.filter(b1__gt=9), update_impl)(objs, ['s2', 'b2'], unfiltered=True)
        self.assertEqual(
            list(MultiSub.objects.all().order_by('pk').values_list('s2', 'b2')),
            [(0, 0), (-1, -1), (-2, -2)]
        )
