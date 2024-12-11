#!/bin/bash

DISK=/dev/sdl

sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
sudo blockdev --flushbufs $DISK
sudo hdparm -F $DISK
free -h
