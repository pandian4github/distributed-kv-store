#!/bin/bash

for i in  `ps aux | grep go | grep server| tr -s ' ' | cut -d ' ' -f2`; do
    kill $i;
done