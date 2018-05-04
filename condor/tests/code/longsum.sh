#!/bin/bash

if [[ $# -ne 2 ]]; then
    echo "Error, need 2 arguments, INPUT OUTPUT"
    exit 1
fi
echo "Reading $1, Writing to $2"
sleep 30
sha256sum "$1" > "$2"
sleep $(( $RANDOM%30 + 30 ))
echo "Done"
