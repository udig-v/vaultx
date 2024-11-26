#!/bin/bash

make clean
make vaultx_x86 NONCE_SIZE=4
for K in {25..31}
do
	./vaultx_x86 -a for -t 64 -K $K -m 16384 -b 8192 -f /data-fast/varvara/vaultx$K.memo.tmp -g /data-l/varvara/vaultx$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
    		./vaultx_x86 -t 64 -L 1000,$hash_size -g vaultx$K.memo
	done
done

make clean 
make vaultx_x86 NONCE_SIZE=5
for K in {32..35}
do
	./vaultx_x86 -a for -t 64 -K $K -m 16384 -b 8192 -f /data-fast/varvara/vaultx$K.memo.tmp -g /data-l/varvara/vaultx$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
    		./vaultx_x86 -t 64 -L 1000,$hash_size -g vaultx$K.memo
	done
done