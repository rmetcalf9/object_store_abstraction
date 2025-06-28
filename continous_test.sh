#!/bin/bash

#pyCharm will run in project root directory. Check if we are here and if so then change int oservices directory
if [ -d "./services" ]; then
  echo "Changing into services directory"
  cd ./services
fi

echo "To run just memory tests run:"
echo "./continous_test.sh test_objectStores_Memory.py"

if [ $# -eq 0 ]; then
  until ack -f --python  ./object_store_abstraction ./tests | entr -d python3 -m pytest ./tests --maxfail=1; do sleep 1; done
else
  if [ $# -eq 1 ]; then
    echo "Testing ${1}"
    until ack -f --python  ./object_store_abstraction ./tests | entr -d python3 -m pytest ./tests/${1} --maxfail=1; do sleep 1; done
  else
    echo "Testing ${1} with verbose option (Single Run)"
    nosetests -v -a ${1} --rednose
  fi
fi
