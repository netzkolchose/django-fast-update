#!/bin/bash
DBENGINE=sqlite coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
DBENGINE=postgres  coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
DBENGINE=mysql coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
DBENGINE=mysql8 coverage run --parallel-mode --source='fast_update' ./manage.py test || exit 1
coverage combine
coverage report
coverage html
