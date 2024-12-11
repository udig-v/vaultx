#!/bin/bash

csv_file="lookup_times_epycbox.csv"
echo "K,Hash_Size,Average_Lookup_Time_ms" > $csv_file

make clean
make vaultx_x86_c NONCE_SIZE=4
for K in {25..31}
do
	#./vaultx -a for -t 64 -K $K -m 16384 -b 8192 -f vaultx$K.memo.tmp -g vaultx$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
		./drop-all-caches.sh
    	output=$(./vaultx -a for -t 64 -b 1000 -p $hash_size -g vaultx$K.memo)

		avg_time=$(echo "$output" | grep -oP '(\d+\.\d+)(?= ms per lookup)')

		if [ -n "$avg_time" ]; then
                        echo "$K,$hash_size,$avg_time" >> $csv_file
                else
                        echo "Error: Could not extract time for K=$K, Hash_Size=$hash_size" >&2
                fi

                #echo "$K,$hash_size,$avg_time" >> $csv_file
	done
done

make clean
make vaultx_x86_c NONCE_SIZE=5
for K in {32..40}
do
	#./vaultx -a for -t 4 -K $K -m 5120 -b 8192 -f /data-fast/varvara/vaultx$K.memo.tmp -g vaultx$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
    		./drop-all-caches.sh
		output=$(./vaultx -a for -t 64 -b 1000 -p $hash_size -g vaultx$K.memo)

		avg_time=$(echo "$output" | grep -oP '(\d+\.\d+)(?= ms per lookup)')

		if [ -n "$avg_time" ]; then
                        echo "$K,$hash_size,$avg_time" >> $csv_file
                else
                        echo "Error: Could not extract time for K=$K, Hash_Size=$hash_size" >&2
                fi
	done
done