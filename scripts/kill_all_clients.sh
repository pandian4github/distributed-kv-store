#!/bin/bash

for i in  `ps aux | grep go | grep client| tr -s ' ' | cut -d ' ' -f2`; do
    kill $i;
done