#!/bin/bash
DBENGINE=sqlite coverage run --parallel-mode --source='fast_update' ./manage.py test
DBENGINE=postgres  coverage run --parallel-mode --source='fast_update' ./manage.py test
DBENGINE=mysql coverage run --parallel-mode --source='fast_update' ./manage.py test
DBENGINE=mysql8 coverage run --parallel-mode --source='fast_update' ./manage.py test
coverage combine
coverage report
coverage html
