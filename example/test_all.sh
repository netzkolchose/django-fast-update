#!/bin/bash

echo
echo "TEST: sqlite 3.33"
DBENGINE=sqlite coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
echo
echo
echo "TEST: postgres"
DBENGINE=postgres  coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
echo
echo
echo "TEST: mariadb"
DBENGINE=mysql coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
echo
echo
echo "TEST: mysql8"
DBENGINE=mysql8 coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
echo
echo
coverage combine
coverage report
coverage html
