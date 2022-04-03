#!/bin/bash

echo "test sqlite 3.33"
DBENGINE=sqlite coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
echo "test postgres"
DBENGINE=postgres  coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
echo "test mariadb"
DBENGINE=mysql coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
echo "test mysql8"
DBENGINE=mysql8 coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
coverage combine
coverage report
coverage html
