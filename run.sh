#!/bin/bash

for K in {25..35}
do
    ./vaultx_x86 -a for -t 64 -K $K -m 16384 -b 8192 -f vaultx$K.memo.tmp -g vaultx$K.memo
done 

for hash_size in 3 4 5 6 7 8 16 32
do
    ./vaultx_x86 -t 64 -L 1000,$hash_size -g vaultx$K.memo
done