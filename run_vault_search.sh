#!/bin/bash

csv_file="search_opi.csv"
echo "K,Hash_Size,Average_Lookup_Time_ms" > $csv_file

make clean
make vault_arm NONCE_SIZE=4
for K in {25..32}
do
	for hash_size in 3 4 5 6 7 8 16 32
	do
		./drop-all-caches.sh
    		output=$(./vault -f vaultx$K.memo -k $K -c 100  -l $hash_size)

                avg_time=$(echo "$output" | grep -oP 'Time taken: \K\d+\.\d+(?= ms per lookup)')

		if [ -n "$avg_time" ]; then
            		echo "$K,$hash_size,$avg_time" >> $csv_file
        	else
            		echo "Error: Could not extract time for K=$K, Hash_Size=$hash_size" >&2
        	fi
	done
done

make clean
make vault_arm NONCE_SIZE=5
for K in {33..35}
do
	for hash_size in 3 4 5 6 7 8 16 32
	do
    		./drop-all-caches.sh
		 output=$(./vault -k $K -f vaultx$K.memo -c 100 -l $hash_size)

		avg_time=$(echo "$output" | grep -oP 'Time taken: \K\d+\.\d+(?= ms per lookup)')

		if [ -n "$avg_time" ]; then
			echo "$K,$hash_size,$avg_time" >> $csv_file
		else
			echo "Error: Could not extract time for K=$K, Hash_Size=$hash_size" >&2
		fi
	done
done
