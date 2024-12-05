#!/bin/bash

<<<<<<< HEAD
#make clean
#make vaultx_arm NONCE_SIZE=4
#for K in {25..31}
#do
#	./vaultx -a for -t 4 -K $K -m 4096 -b 8192 -f vaultx$K.memo.tmp -g vaultx$K.memo
#	for hash_size in 3 4 5 6 7 8 16 32
#	do
#		./drop-all-caches.sh
 #   		./vaultx -t 4 -L 1000,$hash_size -g vaultx$K.memo
#	done
#done
=======
make clean
make vaultx_arm NONCE_SIZE=4
for K in {25..31}
do
	./vaultx -a for -t 8 -K $K -m 16384 -b 8192 -f vaultx$K.memo.tmp -g vaultx$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
		./drop-all-caches.sh
    		./vaultx -t 8 -L 1000,$hash_size -g vaultx$K.memo
	done
done
>>>>>>> 910a58705fc1c3656b0644e33421aa2cc1b3e22b

make clean
make vaultx_arm NONCE_SIZE=5
for K in {33..35}
do
<<<<<<< HEAD
	./vaultx -a for -t 4 -K $K -m 5120 -b 8192 -f /data-fast/varvara/vaultx$K.memo.tmp -g vaultx$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
    		./drop-all-caches.sh
		./vaultx -t 4 -L 1000,$hash_size -g vaultx$K.memo
=======
	./vaultx -a for -t 8 -K $K -m 16384 -b 8192 -f vaultx$K.memo.tmp -g vaultx$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
    		./drop-all-caches.sh
		./vaultx -t 8 -L 1000,$hash_size -g vaultx$K.memo
>>>>>>> 910a58705fc1c3656b0644e33421aa2cc1b3e22b
	done
done
