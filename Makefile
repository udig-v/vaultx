# This Makefile is only for testing. C callers should follow the instructions
# in ./README.md to incorporate these C files into their existing build.

NAME=blake3
#CC=llvm
#CC=gcc
CC=gcc-13
CCP=g++-13
XCC=/ssd-raid0/shared/xgcc/bin/xgcc

CFLAGS=-O3 -DBLAKE3_USE_NEON=0 -Wall -Wextra -pedantic -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIE -fvisibility=hidden
#CFLAGS=-O3 -Wall -Wextra -pedantic -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIE -fvisibility=hidden
LDFLAGS=-lm -lpthread -pie -Wl,-z,relro,-z,now
TARGETS=
ASM_TARGETS=
EXTRAFLAGS=-Wa,--noexecstack

# You can set values with a default, but allow it to be overridden
NONCE_SIZE ?= 5
RECORD_SIZE ?= 8

ifdef BLAKE3_NO_SSE2
EXTRAFLAGS += -DBLAKE3_NO_SSE2
else
TARGETS += blake3_sse2.o
ASM_TARGETS += blake3_sse2_x86-64_unix.S
endif

ifdef BLAKE3_NO_SSE41
EXTRAFLAGS += -DBLAKE3_NO_SSE41
else
TARGETS += blake3_sse41.o
ASM_TARGETS += blake3_sse41_x86-64_unix.S
endif

ifdef BLAKE3_NO_AVX2
EXTRAFLAGS += -DBLAKE3_NO_AVX2
else
TARGETS += blake3_avx2.o
ASM_TARGETS += blake3_avx2_x86-64_unix.S
endif

ifdef BLAKE3_NO_AVX512
EXTRAFLAGS += -DBLAKE3_NO_AVX512
else
TARGETS += blake3_avx512.o
ASM_TARGETS += blake3_avx512_x86-64_unix.S
endif

ifdef BLAKE3_USE_NEON
EXTRAFLAGS += -DBLAKE3_USE_NEON=1
TARGETS += blake3_neon.o
endif

ifdef BLAKE3_NO_NEON
EXTRAFLAGS += -DBLAKE3_USE_NEON=0
endif

all: blake3.c blake3_dispatch.c blake3_portable.c main.c $(TARGETS)
	$(CC) $(CFLAGS) $(EXTRAFLAGS) $^ -o $(NAME) $(LDFLAGS)

blake3_sse2.o: blake3_sse2.c
	$(CC) $(CFLAGS) $(EXTRAFLAGS) -c $^ -o $@ -msse2

blake3_sse41.o: blake3_sse41.c
	$(CC) $(CFLAGS) $(EXTRAFLAGS) -c $^ -o $@ -msse4.1

blake3_avx2.o: blake3_avx2.c
	$(CC) $(CFLAGS) $(EXTRAFLAGS) -c $^ -o $@ -mavx2

blake3_avx512.o: blake3_avx512.c
	$(CC) $(CFLAGS) $(EXTRAFLAGS) -c $^ -o $@ -mavx512f -mavx512vl

blake3_neon.o: blake3_neon.c
	$(CC) $(CFLAGS) $(EXTRAFLAGS) -c $^ -o $@

test: CFLAGS += -DBLAKE3_TESTING -fsanitize=address,undefined
test: all
	./test.py

asm: blake3.c blake3_dispatch.c blake3_portable.c main.c $(ASM_TARGETS)
	$(CC) $(CFLAGS) $(EXTRAFLAGS) $^ -o $(NAME) $(LDFLAGS)

test_asm: CFLAGS += -DBLAKE3_TESTING -fsanitize=address,undefined 
test_asm: asm
	./test.py

vault_x86: vault.c blake3.c blake3_dispatch.c blake3_portable.c $(ASM_TARGETS)
	$(CC) $(CFLAGS) $(EXTRAFLAGS) $^ -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) -o vault $(LDFLAGS)

vaultx_x86: vaultx.c blake3.c blake3_dispatch.c blake3_portable.c $(ASM_TARGETS)
	$(CCP) -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) $(CFLAGS) $(EXTRAFLAGS) $^ -x c++ -std=c++17 -o vaultx $(LDFLAGS) -fopenmp -ltbb

vaultx_x86_c: vaultx.c blake3.c blake3_dispatch.c blake3_portable.c $(ASM_TARGETS)
	$(CC) -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) -I/usr/include $(CFLAGS) $(EXTRAFLAGS) $^ -o vaultx $(LDFLAGS) -fopenmp

vaultx_x86_xgcc: vaultx.c blake3.c blake3_dispatch.c blake3_portable.c $(ASM_TARGETS)
	$(XCC) -I/ssd-raid0/shared/xgcc/include/ -I/ssd-raid0/shared/xgcc/lib/gcc/x86_64-pc-linux-gnu/12.2.1/include/ -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) $(CFLAGS) $(EXTRAFLAGS) $^ -o $@ $(LDFLAGS) -fopenmp 

vault_arm: vault.c blake3.c blake3_dispatch.c blake3_portable.c
	$(CC) $(CFLAGS) $(EXTRAFLAGS) $^ -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) -o vault $(LDFLAGS)

vaultx_arm: vaultx.c blake3.c blake3_dispatch.c blake3_portable.c
	$(CCP) -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) $(CFLAGS) $(EXTRAFLAGS) $^ -x c++ -std=c++17 -o vaultx $(LDFLAGS) -fopenmp -ltbb 

vaultx_arm_c: vaultx.c blake3.c blake3_dispatch.c blake3_portable.c
	$(CC) -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) $(CFLAGS) $(EXTRAFLAGS) $^ -o vaultx $(LDFLAGS) -fopenmp


vault_mac: vault.c
#	$(CC) -o vault vault.c -lblake3 -lsqlite3 -lpthread -O3  -I/opt/homebrew/opt/blake3/include -L/opt/homebrew/opt/blake3/lib
	$(CC) -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) -o vault vault.c -lblake3 -lpthread -ltbb -O3  -I/opt/homebrew/opt/blake3/include -L/opt/homebrew/opt/blake3/lib  -I/opt/homebrew/opt/tbb/include -L/opt/homebrew/opt/tbb/lib

vaultx_mac: vaultx.c
#-D NONCE_SIZE=$(NONCE_SIZE) 
	$(CCP) -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) -x c++ -std=c++17 -o vaultx vaultx.c -fopenmp -lblake3 -ltbb -O3  -I/opt/homebrew/opt/blake3/include -L/opt/homebrew/opt/blake3/lib -I/opt/homebrew/opt/tbb/include -L/opt/homebrew/opt/tbb/lib

vaultx_mac_c: vaultx.c
#-D NONCE_SIZE=$(NONCE_SIZE) 
	$(CC) -DNONCE_SIZE=$(NONCE_SIZE) -DRECORD_SIZE=$(RECORD_SIZE) -o vaultx vaultx.c -fopenmp -lblake3 -O3  -I/opt/homebrew/opt/blake3/include -L/opt/homebrew/opt/blake3/lib


clean: 
	rm -f $(NAME) vault vaultx vault_* vaultx_* *.o

