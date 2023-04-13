#!/bin/bash

echo -e "\nTEST: sqlite"
DBENGINE=sqlite coverage run --parallel-mode --source='fast_update' ./example/manage.py test exampleapp || exit 1
echo -e "\nTEST: postgres"
DBENGINE=postgres  coverage run --parallel-mode --source='fast_update' ./example/manage.py test exampleapp || exit 1
DBENGINE=postgres  coverage run --parallel-mode --source='fast_update' ./example/manage.py test postgres_tests || exit 1
echo -e "\nTEST: mariadb"
DBENGINE=mysql coverage run --parallel-mode --source='fast_update' ./example/manage.py test exampleapp || exit 1
echo -e "\nTEST: mysql8"
DBENGINE=mysql8 coverage run --parallel-mode --source='fast_update' ./example/manage.py test exampleapp || exit 1
coverage combine
coverage report
coverage html
