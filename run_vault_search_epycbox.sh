#!/bin/bash

csv_file="search_epycbox.csv"
echo "K,Hash_Size,Average_Lookup_Time_ms" > $csv_file

make clean
make vault_x86 NONCE_SIZE=4 RECORD_SIZE=32
for K in {25..32}
do
	./vault -t 8 -o 64 -i 1 -m 262144 -k $K -f vault$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
		./drop-all-caches.sh
    		output=$(./vault -f vault$K.memo -k $K -c 100  -l $hash_size)

                avg_time=$(echo "$output" | grep -oP 'Time taken: \K\d+\.\d+(?= ms per lookup)')

		if [ -n "$avg_time" ]; then
            		echo "$K,$hash_size,$avg_time" >> $csv_file
        	else
            		echo "Error: Could not extract time for K=$K, Hash_Size=$hash_size" >&2
        	fi
	done
done

make clean
make vault_x86 NONCE_SIZE=5 RECORD_SIZE=32
for K in {33..35}
do
	./vault -t 8 -o 64 -i 1 -m 262144 -k $K -f vault$K.memo
	for hash_size in 3 4 5 6 7 8 16 32
	do
    		./drop-all-caches.sh
		output=$(./vault -k $K -f vault$K.memo -c 100 -l $hash_size)

		avg_time=$(echo "$output" | grep -oP 'Time taken: \K\d+\.\d+(?= ms per lookup)')

		if [ -n "$avg_time" ]; then
			echo "$K,$hash_size,$avg_time" >> $csv_file
		else
			echo "Error: Could not extract time for K=$K, Hash_Size=$hash_size" >&2
		fi
	done
done
