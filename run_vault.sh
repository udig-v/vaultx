#!/bin/bash

output_file="output2.csv"
echo "K,RAM,HASH,SORT,FLUSH,COMPRESS,TOTAL" > $output_file

RAM=16384

#make clean
#make vault_arm NONCE_SIZE=4 RECORD_SIZE=32

#for k in {25..32}
#do
#	output=$(./vault -t 8 -o 8 -i 8 -f vault$k.memo -m $RAM -k $k -w true)
#	echo "$k,$RAM,$output" >> $output_file
#done

make clean
make vault_arm NONCE_SIZE=5 RECORD_SIZE=32
for k in {33..35}
do
	output=$(./vault -t 8 -o 8 -i 8 -f vault$k.memo -m $RAM -k $k -w true)
	echo "$k,$RAM,$output" >> $output_file
done
