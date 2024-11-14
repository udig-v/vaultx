Vaultx - OpenMP parallel PoS implementation in C

Compile (you can change the compiler in the makefile)
make vaultx_x86 NONCE_SIZE=4

You can also compile for ARM or MAC with:
make vaultx_mac NONCE_SIZE=4
make vaultx_arm NONCE_SIZE=4

To run (hash generation without I/O):
./vaultx -a for -t 16 -K 32 -m 1024 -b 1024

Sample output on my laptop:
Selected Approach           : for
Number of Threads           : 16
Exponent K                  : 32
File Size (GB)              : 16.00
File Size (bytes)           : 17179869184
Memory Size (MB)            : 1024
Memory Size (bytes)         : 1073741824
Number of Hashes (RAM)      : 268435456
Number of Hashes (Disk)     : 4294967296
Size of MemoRecord          : 4
Rounds                      : 16
Number of Buckets           : 16777216
Number of Records in Bucket : 16
BATCH_SIZE                  : 1024
[6.94] HashGen 6.25%: 40.90 MH/s : I/O 
[13.74] HashGen 12.50%: 39.46 MH/s : I/O 
[20.66] HashGen 18.75%: 38.79 MH/s : I/O 
[27.32] HashGen 25.00%: 40.28 MH/s : I/O 
[34.09] HashGen 31.25%: 39.65 MH/s : I/O 
[40.87] HashGen 37.50%: 39.60 MH/s : I/O 
[47.25] HashGen 43.75%: 42.12 MH/s : I/O 
[53.68] HashGen 50.00%: 41.69 MH/s : I/O 
[60.17] HashGen 56.25%: 41.38 MH/s : I/O 
[67.22] HashGen 62.50%: 38.11 MH/s : I/O 
[73.85] HashGen 68.75%: 40.43 MH/s : I/O 
[80.38] HashGen 75.00%: 41.14 MH/s : I/O 
[86.91] HashGen 81.25%: 41.12 MH/s : I/O 
[93.31] HashGen 87.50%: 41.96 MH/s : I/O 
[99.89] HashGen 93.75%: 40.75 MH/s : I/O 
[106.25] HashGen 100.00%: 42.25 MH/s : I/O 
Total Throughput: 40.34 MH/s  161.36 MB/s
Total Time: 106.470349 seconds

The command line arguments are as follows:
-a for/task: parallel model, for loop or task model
-t 16: number of threads in OpenMP
-K 32: number of hashes to generate, 2^k; 32 will give you 4B hashes
-m 1024: amount of memory in MB to store the hashes
-b 1024: batch size (to control the task/loop iteration size)


