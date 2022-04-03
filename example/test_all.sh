#!/bin/bash

echo -e "\nTEST: sqlite 3.33"
DBENGINE=sqlite coverage run --parallel-mode --source='fast_update' ./example/manage.py test || exit 1
echo -e "\nTEST: postgres"
DBENGINE=postgres  coverage run --parallel-mode --source='fast_update' ./example/manage.py test || exit 1
echo -e "\nTEST: mariadb"
DBENGINE=mysql coverage run --parallel-mode --source='fast_update' ./example/manage.py test || exit 1
echo -e "\nTEST: mysql8"
DBENGINE=mysql8 coverage run --parallel-mode --source='fast_update' ./example/manage.py test || exit 1
coverage combine
coverage report
#coverage html
