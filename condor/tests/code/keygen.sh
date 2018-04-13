#!/bin/bash

bits=$1
pwd
ls

ssh-keygen -G "candidates.$bits" -b $bits -M 8
ssh-keygen -T "moduli.$bits" -f "candidates.$bits"
