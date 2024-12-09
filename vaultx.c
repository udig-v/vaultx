#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <time.h>
#include <unistd.h> // For getpid()
#include <string.h> // For strcmp
#include <getopt.h> // For getopt_long
#include <stdbool.h>
#include <fcntl.h>     // For open, O_RDWR, O_CREAT, O_TRUNC
#include <sys/types.h> // For data types
#include <sys/stat.h>  // For file modes
#include <math.h>
#include <errno.h>

#ifdef __linux__
#include <linux/fs.h> // Provides `syncfs` on Linux
#endif

#ifdef __cplusplus
// Your C++-specific code here
#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>
#endif

#include "blake3.h" // Include Blake3 header

#ifndef NONCE_SIZE
#define NONCE_SIZE 5 // Default nonce size
#endif

#ifndef RECORD_SIZE
#define RECORD_SIZE 8 // Default record size
#endif

#define HASH_SIZE (RECORD_SIZE - NONCE_SIZE)
#define PREFIX_SIZE 3 // Example prefix size for getBucketIndex

unsigned long long num_buckets = 1;
unsigned long long num_records_in_bucket = 1;
unsigned long long rounds = 1;

size_t BATCH_SIZE = 1024;

bool VERIFY = false;
bool DEBUG = false;
bool writeData = false;
bool writeDataFinal = false;
bool MEMORY_WRITE = true;
bool CIRCULAR_ARRAY = false;
bool BENCHMARK = false;
bool HASHGEN = true;
bool SEARCH = false;
bool SEARCH_BATCH = false;
size_t PREFIX_SEARCH_SIZE = 1;
int NUM_THREADS = 0;

// Structure to hold a record with nonce and hash
typedef struct
{
    uint8_t hash[HASH_SIZE];   // 32-byte Blake3 hash
    uint8_t nonce[NONCE_SIZE]; // Nonce to store the seed
} MemoAllRecord;

// Structure to hold a record with nonce
typedef struct
{
    uint8_t nonce[NONCE_SIZE]; // Nonce to store the seed
} MemoRecord;

typedef struct
{
    MemoRecord *records;
    size_t count; // Number of records in the bucket
    size_t flush; // Number of flushes of bucket
} Bucket;

// Function to display usage information
void print_usage(char *prog_name)
{
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("\nOptions:\n");
    printf("  -a, --approach [task|for]    Select parallelization approach (default: for)\n");
    printf("  -t, --threads NUM            Number of threads to use (default: number of available cores)\n");
    printf("  -K, --exponent NUM           Exponent K to compute iterations as 2^K (default: 4)\n");
    printf("  -m, --memory NUM             Memory size in MB (default: 1)\n");
    printf("  -f, --file NAME              Output file name\n");
    printf("  -b, --batch-size NUM         Batch size (default: 1024)\n");
    printf("  -h, --help                   Display this help message\n");
    printf("\nExample:\n");
    printf("  %s -a task -t 8 -K 20 -m 1024 -f output.dat\n", prog_name);
}

// Function to compute the bucket index based on hash prefix
off_t getBucketIndex(const uint8_t *hash, size_t prefix_size)
{
    off_t index = 0;
    for (size_t i = 0; i < prefix_size && i < HASH_SIZE; i++)
    {
        index = (index << 8) | hash[i];
    }
    return index;
}

// Function to convert bytes to unsigned long long
unsigned long long byteArrayToLongLong(const uint8_t *byteArray, size_t length)
{
    unsigned long long result = 0;
    for (size_t i = 0; i < length; ++i)
    {
        result = (result << 8) | (unsigned long long)byteArray[i];
    }
    return result;
}

// Function to generate Blake3 hash
void generateBlake3(uint8_t *record_hash, MemoRecord *record, unsigned long long seed)
{
    // Ensure that the pointers are valid
    if (record_hash == NULL)
    {
        fprintf(stderr, "Error: NULL pointer passed to generateBlake3.\n");
        return;
    }

    // Store seed into the nonce
    memcpy(record->nonce, &seed, NONCE_SIZE);

    // Generate Blake3 hash
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, record->nonce, NONCE_SIZE);
    blake3_hasher_finalize(&hasher, record_hash, HASH_SIZE);
}

// Function to write a bucket of records to disk sequentially
size_t writeBucketToDiskSequential(const Bucket *bucket, FILE *fd)
{
    // printf("num_records_in_bucket=%llu sizeof(MemoRecord)=%d\n",num_records_in_bucket,sizeof(MemoRecord));
    size_t elementsWritten = fwrite(bucket->records, sizeof(MemoRecord), num_records_in_bucket, fd);
    if (elementsWritten != num_records_in_bucket)
    {
        fprintf(stderr, "Error writing bucket to file; elements written %zu when expected %llu\n",
                elementsWritten, num_records_in_bucket);
        fclose(fd);
        exit(EXIT_FAILURE);
    }
    return elementsWritten * sizeof(MemoRecord);
}

// Function to insert a record into a bucket
void insert_record(Bucket *buckets, MemoRecord *record, size_t bucketIndex)
{
    if (bucketIndex >= num_buckets)
    {
        fprintf(stderr, "Error: Bucket index %zu out of range (0 to %llu).\n", bucketIndex, num_buckets - 1);
        return;
    }

    Bucket *bucket = &buckets[bucketIndex];

    // Protect count increment with atomic operation
    size_t idx;
#pragma omp atomic capture
    idx = bucket->count++;

    // Check if there's room in the bucket
    if (idx < num_records_in_bucket)
    {
        memcpy(bucket->records[idx].nonce, record->nonce, NONCE_SIZE);
    }
    else
    {
        // Bucket is full; handle overflow if necessary
        // For now, we ignore overflow
    }
}

// Function to concatenate two strings and return the result
char *concat_strings(const char *str1, const char *str2)
{
    // Check for NULL pointers
    if (str1 == NULL || str2 == NULL)
    {
        fprintf(stderr, "Error: NULL string passed to concat_strings.\n");
        return NULL;
    }

    // Calculate the lengths of the input strings
    size_t len1 = strlen(str1);
    size_t len2 = strlen(str2);

    // Allocate memory for the concatenated string (+1 for the null terminator)
    char *result = (char *)malloc(len1 + len2 + 1);
    if (result == NULL)
    {
        fprintf(stderr, "Error: Memory allocation failed in concat_strings.\n");
        return NULL;
    }

    // Copy the first string into the result
    strcpy(result, str1);

    // Append the second string to the result
    strcat(result, str2);

    return result;
}

// Function to check if a nonce is non-zero
bool is_nonce_nonzero(const uint8_t *nonce, size_t nonce_size)
{
    // Check for NULL pointer
    if (nonce == NULL)
    {
        // Handle error as needed
        return false;
    }

    // Iterate over each byte of the nonce
    for (size_t i = 0; i < nonce_size; ++i)
    {
        if (nonce[i] != 0)
        {
            // Found a non-zero byte
            return true;
        }
    }

    // All bytes are zero
    return false;
}

// Function to count zero-value MemoRecords in a binary file
size_t count_zero_memo_records(const char *filename)
{
    const size_t BATCH_SIZE = 1000000; // 1 million MemoRecords per batch
    MemoRecord *buffer = NULL;
    size_t total_zero_records = 0;
    size_t total_nonzero_records = 0;
    size_t records_read;
    FILE *file = NULL;

    // Open the file for reading in binary mode
    file = fopen(filename, "rb");
    if (file == NULL)
    {

        printf("Error opening file %s (#1)\n", filename);
        return 0;
    }

    // Allocate memory for the batch of MemoRecords
    buffer = (MemoRecord *)malloc(BATCH_SIZE * sizeof(MemoRecord));
    if (buffer == NULL)
    {
        fprintf(stderr, "Error: Unable to allocate memory.\n");
        fclose(file);
        return 0;
    }

    // Read the file in batches
    while ((records_read = fread(buffer, sizeof(MemoRecord), BATCH_SIZE, file)) > 0)
    {
        // Process each MemoRecord in the batch
        for (size_t i = 0; i < records_read; ++i)
        {
            // Check if the MemoRecord's nonce is all zeros
            if (is_nonce_nonzero(buffer[i].nonce, NONCE_SIZE))
            {
                ++total_nonzero_records;
            }
            else
            {
                ++total_zero_records;
            }
        }
    }

    // Check for reading errors
    if (ferror(file))
    {
        perror("Error reading file");
    }

    // Clean up
    fclose(file);
    free(buffer);

    // Print the total number of zero-value MemoRecords
    printf("total_zero_records=%zu total_nonzero_records=%zu efficiency=%.2f%%\n", total_zero_records, total_nonzero_records, total_nonzero_records * 100.0 / (total_zero_records + total_nonzero_records));

    return total_zero_records;
}

long get_file_size(const char *filename)
{
    FILE *file = fopen(filename, "rb"); // Open the file in binary mode
    long size;

    if (file == NULL)
    {
        printf("Error opening file %s (#2)\n", filename);

        perror("Error opening file");
        return -1;
    }

    // Move the file pointer to the end of the file
    if (fseek(file, 0, SEEK_END) != 0)
    {
        perror("Error seeking to end of file");
        fclose(file);
        return -1;
    }

    // Get the current position in the file, which is the size
    size = ftell(file);
    if (size == -1L)
    {
        perror("Error getting file position");
        fclose(file);
        return -1;
    }

    fclose(file);
    return size;
}

size_t process_memo_records(const char *filename, const size_t BATCH_SIZE)
{
    // const size_t BATCH_SIZE = 1000000; // 1 million MemoRecords per batch
    MemoRecord *buffer = NULL;
    size_t total_records = 0;
    size_t zero_nonce_count = 0;
    size_t records_read;
    FILE *file = NULL;
    uint8_t prev_hash[PREFIX_SIZE] = {0}; // Initialize previous hash prefix to zero
    uint8_t prev_nonce[NONCE_SIZE] = {0}; // Initialize previous nonce to zero
    size_t count_condition_met = 0;       // Counter for records meeting the condition
    size_t count_condition_not_met = 0;

    long filesize = get_file_size(filename);

    if (filesize != -1)
    {
        if (!BENCHMARK)
            printf("Size of '%s' is %ld bytes.\n", filename, filesize);
    }

    // Open the file for reading in binary mode
    file = fopen(filename, "rb");
    if (file == NULL)
    {
        printf("Error opening file %s (#3)\n", filename);

        perror("Error opening file");
        return 0;
    }

    // Allocate memory for the batch of MemoRecords
    buffer = (MemoRecord *)malloc(BATCH_SIZE * sizeof(MemoRecord));
    if (buffer == NULL)
    {
        fprintf(stderr, "Error: Unable to allocate memory.\n");
        fclose(file);
        return 0;
    }

    // Start walltime measurement
    double start_time = omp_get_wtime();
    // double end_time = omp_get_wtime();

    // Read the file in batches
    while ((records_read = fread(buffer, sizeof(MemoRecord), BATCH_SIZE, file)) > 0)
    {
        double start_time_verify = omp_get_wtime();
        double end_time_verify = omp_get_wtime();

        // Process each MemoRecord in the batch
        for (size_t i = 0; i < records_read; ++i)
        {
            ++total_records;

            if (is_nonce_nonzero(buffer[i].nonce, NONCE_SIZE))
            {
                uint8_t hash_output[HASH_SIZE];

                // Compute Blake3 hash of the nonce
                blake3_hasher hasher;
                blake3_hasher_init(&hasher);
                blake3_hasher_update(&hasher, buffer[i].nonce, NONCE_SIZE);
                blake3_hasher_finalize(&hasher, hash_output, HASH_SIZE);

                // Compare the first PREFIX_SIZE bytes of the current hash to the previous hash prefix
                if (memcmp(hash_output, prev_hash, PREFIX_SIZE) >= 0)
                {
                    // Current hash's first PREFIX_SIZE bytes are equal to or greater than previous
                    ++count_condition_met;
                }
                else
                {
                    ++count_condition_not_met;

                    if (DEBUG)
                    {
                        // Print previous hash and nonce, and current hash and nonce
                        printf("Condition not met at record %zu:\n", total_records);
                        printf("Previous nonce: ");
                        for (size_t n = 0; n < NONCE_SIZE; ++n)
                            printf("%02X", prev_nonce[n]);
                        printf("\n");
                        printf("Previous hash prefix: ");
                        for (size_t n = 0; n < PREFIX_SIZE; ++n)
                            printf("%02X", prev_hash[n]);
                        printf("\n");

                        printf("Current nonce: ");
                        for (size_t n = 0; n < NONCE_SIZE; ++n)
                            printf("%02X", buffer[i].nonce[n]);
                        printf("\n");
                        printf("Current hash prefix: ");
                        for (size_t n = 0; n < PREFIX_SIZE; ++n)
                            printf("%02X", hash_output[n]);
                        printf("\n");
                    }
                }

                // Update the previous hash prefix and nonce
                memcpy(prev_hash, hash_output, PREFIX_SIZE);
                memcpy(prev_nonce, buffer[i].nonce, NONCE_SIZE);
            }
            else
            {
                ++zero_nonce_count;
                // Optionally, handle zero nonces here
            }
        }
        end_time_verify = omp_get_wtime();
        double elapsed_time_verify = end_time_verify - start_time_verify;
        double elapsed_time = omp_get_wtime() - start_time;

        // Calculate throughput (hashes per second)
        double throughput = (BATCH_SIZE * sizeof(MemoRecord) / elapsed_time_verify) / (1024 * 1024);
        printf("[%.2f] Verify %.2f%%: %.2f MB/s\n", elapsed_time, total_records * sizeof(MemoRecord) * 100.0 / filesize, throughput);
    }

    // Check for reading errors
    if (ferror(file))
    {
        perror("Error reading file");
    }

    // Clean up
    fclose(file);
    free(buffer);

    // Print the total number of times the condition was met
    printf("sorted=%zu not_sorted=%zu zero_nonces=%zu total_records=%zu storage_efficiency=%.2f%%\n",
           count_condition_met, count_condition_not_met, zero_nonce_count, total_records, count_condition_met * 100.0 / total_records);

    return count_condition_met;
}

/**
 * Converts a given string to an array of uint8_t.
 *
 * @param SEARCH_STRING The input string to convert.
 * @return A pointer to the array of uint8_t, or NULL if allocation fails.
 */
uint8_t *convert_string_to_uint8_array(const char *SEARCH_STRING)
{
    if (SEARCH_STRING == NULL)
    {
        return NULL;
    }

    size_t length = strlen(SEARCH_STRING);
    uint8_t *array = (uint8_t *)malloc(length * sizeof(uint8_t));
    if (array == NULL)
    {
        // Memory allocation failed
        return NULL;
    }

    for (size_t i = 0; i < length; ++i)
    {
        array[i] = (uint8_t)SEARCH_STRING[i];
    }

    return array;
}

uint8_t *hexStringToByteArray(const char *hexString)
{

    size_t hexLen = strlen(hexString);
    uint8_t *byteArray = (uint8_t *)malloc(hexLen * sizeof(uint8_t));
    // size_t hexLen = strlen(hexString);
    if (hexLen % 2 != 0)
    {
        return NULL; // Error: Invalid hexadecimal string length
    }

    size_t byteLen = hexLen / 2;
    size_t byteArraySize = byteLen;
    if (byteLen > byteArraySize)
    {
        return NULL; // Error: Byte array too small
    }

    for (size_t i = 0; i < byteLen; ++i)
    {
        if (sscanf(&hexString[i * 2], "%2hhx", &byteArray[i]) != 1)
        {
            return NULL; // Error: Failed to parse hexadecimal string
        }
    }

    return byteArray;
}

long long search_memo_record(FILE *file, off_t bucketIndex, uint8_t *SEARCH_UINT8, size_t SEARCH_LENGTH, unsigned long long num_records_in_bucket_search, MemoRecord *buffer)
{
    const int HASH_SIZE_SEARCH = 8;
    // unsigned long long fRecord = -1;
    size_t records_read;
    unsigned long long foundRecord = -1;
    // Define the offset you want to seek to
    long offset = bucketIndex * num_records_in_bucket_search * sizeof(MemoRecord); // For example, seek to byte 1024 from the beginning
    if (DEBUG)
        printf("SEARCH: seek to %zu offset\n", offset);

    // Seek to the specified offset
    if (fseek(file, offset, SEEK_SET) != 0)
    {
        perror("Error seeking in file");
        fclose(file);
        return -1;
    }

    records_read = fread(buffer, sizeof(MemoRecord), num_records_in_bucket_search, file);
    if (records_read > 0)
    {
        int found = 0; // Shared flag to indicate termination

#pragma omp parallel shared(found)
        {
#pragma omp for
            for (size_t i = 0; i < records_read; ++i)
            {
                //++total_records;

                // Check for cancellation
#pragma omp cancellation point for
                if (!found && is_nonce_nonzero(buffer[i].nonce, NONCE_SIZE))
                {
                    uint8_t hash_output[HASH_SIZE_SEARCH];

                    // Compute Blake3 hash of the nonce
                    blake3_hasher hasher;
                    blake3_hasher_init(&hasher);
                    blake3_hasher_update(&hasher, buffer[i].nonce, NONCE_SIZE);
                    blake3_hasher_finalize(&hasher, hash_output, HASH_SIZE_SEARCH);

                    // print bucket contents
                    if (DEBUG)
                    {

                        printf("bucket[");

                        // printf("Search hash prefix (UINT8): ");
                        for (size_t n = 0; n < PREFIX_SIZE; ++n)
                            printf("%02X", SEARCH_UINT8[n]);
                        printf("][%zu] = ", i);

                        // printf("Current nonce: ");
                        for (size_t n = 0; n < NONCE_SIZE; ++n)
                            printf("%02X", buffer[i].nonce[n]);
                        printf(" => ");
                        // printf("Current hash prefix: ");
                        for (size_t n = 0; n < HASH_SIZE_SEARCH; ++n)
                            printf("%02X", hash_output[n]);
                        printf("\n");
                    }

                    // Compare the first PREFIX_SIZE bytes of the current hash to the previous hash prefix
                    if (memcmp(hash_output, SEARCH_UINT8, SEARCH_LENGTH) == 0)
                    {
                        // Current hash's first PREFIX_SIZE bytes are equal to or greater than previous
                        //++count_condition_met;
                        // fRecord = buffer[i];
                        // foundRecord = true;
                        // return byteArrayToLongLong(buffer[i].nonce,NONCE_SIZE);
                        // Signal cancellation

                        foundRecord = byteArrayToLongLong(buffer[i].nonce, NONCE_SIZE);

#pragma omp atomic write

                        // if (!DEBUG)
                        //{

                        found = 1;

#pragma omp cancel for
                        //}
                    }
                    else
                    {
                        //++count_condition_not_met;

                        /*
                                            if (!DEBUG)
                                            {
                                            // Print previous hash and nonce, and current hash and nonce
                                            //printf("Condition not met at record %zu:\n", total_records);
                                            //printf("Search string %s\n",SEARCH_STRING);

                                            printf("Search hash prefix (UINT8): ");
                                            for (size_t n = 0; n < PREFIX_SIZE; ++n)
                                                printf("%02X", SEARCH_UINT8[n]);
                                            printf("\n");

                                            printf("Current nonce: ");
                                            for (size_t n = 0; n < NONCE_SIZE; ++n)
                                                printf("%02X", buffer[i].nonce[n]);
                                            printf("\n");
                                            printf("Current hash prefix: ");
                                            for (size_t n = 0; n < HASH_SIZE_SEARCH; ++n)
                                                printf("%02X", hash_output[n]);
                                            printf("\n");
                                            }
                                            */
                    }
                }
            }
        }
    }
    else
    {
        printf("error reading from file..\n");
    }
    return foundRecord;
}

// not sure if the search of more than PREFIX_LENGTH works
void search_memo_records(const char *filename, const char *SEARCH_STRING)
{

    uint8_t *SEARCH_UINT8 = hexStringToByteArray(SEARCH_STRING);
    size_t SEARCH_LENGTH = strlen(SEARCH_STRING) / 2;
    off_t bucketIndex = getBucketIndex(SEARCH_UINT8, PREFIX_SIZE);
    // uint8_t *SEARCH_UINT8 = convert_string_to_uint8_array(SEARCH_STRING);
    // num_records_in_bucket
    MemoRecord *buffer = NULL;
    // size_t total_records = 0;
    // size_t zero_nonce_count = 0;

    FILE *file = NULL;
    // uint8_t prev_hash[PREFIX_SIZE] = {0}; // Initialize previous hash prefix to zero
    // uint8_t prev_nonce[NONCE_SIZE] = {0}; // Initialize previous nonce to zero
    // size_t count_condition_met = 0;       // Counter for records meeting the condition
    // size_t count_condition_not_met = 0;
    bool foundRecord = false;
    // MemoRecord fRecord;
    long long fRecord = -1;

    long filesize = get_file_size(filename);

    if (filesize != -1)
    {
        if (!BENCHMARK)
            printf("Size of '%s' is %ld bytes.\n", filename, filesize);
    }

    unsigned long long num_buckets_search = 1ULL << (PREFIX_SIZE * 8);
    unsigned long long num_records_in_bucket_search = filesize / num_buckets_search / sizeof(MemoRecord);
    if (!BENCHMARK)
    {
        printf("SEARCH: filename=%s\n", filename);
        printf("SEARCH: filesize=%zu\n", filesize);
        printf("SEARCH: num_buckets=%lluu\n", num_buckets_search);
        printf("SEARCH: num_records_in_bucket=%llu\n", num_records_in_bucket_search);
        printf("SEARCH: SEARCH_STRING=%s\n", SEARCH_STRING);
    }

    // Open the file for reading in binary mode
    file = fopen(filename, "rb");
    if (file == NULL)
    {
        printf("Error opening file %s (#3)\n", filename);

        perror("Error opening file");
        return;
    }

    // Allocate memory for the batch of MemoRecords
    buffer = (MemoRecord *)malloc(num_records_in_bucket_search * sizeof(MemoRecord));
    if (buffer == NULL)
    {
        fprintf(stderr, "Error: Unable to allocate memory.\n");
        fclose(file);
        return;
    }

    // Start walltime measurement
    double start_time = omp_get_wtime();
    // double end_time = omp_get_wtime();

    fRecord = search_memo_record(file, bucketIndex, SEARCH_UINT8, SEARCH_LENGTH, num_records_in_bucket_search, buffer);
    if (fRecord >= 0)
        foundRecord = true;
    else
        foundRecord = false;

    double elapsed_time = (omp_get_wtime() - start_time) * 1000.0;

    // Check for reading errors
    if (ferror(file))
    {
        perror("Error reading file");
    }

    // Clean up
    fclose(file);
    free(buffer);

    // Print the total number of times the condition was met
    if (foundRecord == true)
        printf("NONCE found (%llu) for HASH prefix %s\n", fRecord, SEARCH_STRING);
    else
        printf("no NONCE found for HASH prefix %s\n", SEARCH_STRING);
    printf("search time %.2f ms\n", elapsed_time);

    // return NULL;
}

// not sure if the search of more than PREFIX_LENGTH works
void search_memo_records_batch(const char *filename, int num_lookups, int search_size)
{

    // Seed the random number generator with the current time
    srand((unsigned int)time(NULL));

    // uint8_t *SEARCH_UINT8 = hexStringToByteArray("000000");
    size_t SEARCH_LENGTH = search_size;
    // uint8_t *SEARCH_UINT8 = convert_string_to_uint8_array(SEARCH_STRING);
    // num_records_in_bucket
    MemoRecord *buffer = NULL;
    // size_t total_records = 0;
    // size_t zero_nonce_count = 0;

    FILE *file = NULL;
    // uint8_t prev_hash[PREFIX_SIZE] = {0}; // Initialize previous hash prefix to zero
    // uint8_t prev_nonce[NONCE_SIZE] = {0}; // Initialize previous nonce to zero
    // size_t count_condition_met = 0;       // Counter for records meeting the condition
    // size_t count_condition_not_met = 0;
    int foundRecords = 0;
    int notFoundRecords = 0;
    // MemoRecord fRecord;
    // long long fRecord = -1;

    long filesize = get_file_size(filename);

    if (filesize != -1)
    {
        if (!BENCHMARK)
            printf("Size of '%s' is %ld bytes.\n", filename, filesize);
    }

    unsigned long long num_buckets_search = 1ULL << (PREFIX_SIZE * 8);
    unsigned long long num_records_in_bucket_search = filesize / num_buckets_search / sizeof(MemoRecord);
    if (!BENCHMARK)
    {
        printf("SEARCH: filename=%s\n", filename);
        printf("SEARCH: filesize=%zu\n", filesize);
        printf("SEARCH: num_buckets=%llu\n", num_buckets_search);
        printf("SEARCH: num_records_in_bucket=%llu\n", num_records_in_bucket_search);
    }
    // printf("SEARCH: SEARCH_STRING=%s\n",SEARCH_STRING);

    // Open the file for reading in binary mode
    file = fopen(filename, "rb");
    if (file == NULL)
    {
        printf("Error opening file %s (#3)\n", filename);

        perror("Error opening file");
        return;
    }

    // Allocate memory for the batch of MemoRecords
    buffer = (MemoRecord *)malloc(num_records_in_bucket_search * sizeof(MemoRecord));
    if (buffer == NULL)
    {
        fprintf(stderr, "Error: Unable to allocate memory.\n");
        fclose(file);
        return;
    }

    // Start walltime measurement
    double start_time = omp_get_wtime();
    // double end_time = omp_get_wtime();

    uint8_t SEARCH_UINT8[search_size];

    for (int i = 0; i < num_lookups; i++)
    {

        for (int i = 0; i < search_size; ++i)
        {
            SEARCH_UINT8[i] = rand() % 256;
        }

        if (search_memo_record(file, getBucketIndex(SEARCH_UINT8, PREFIX_SIZE), SEARCH_UINT8, SEARCH_LENGTH, num_records_in_bucket_search, buffer) >= 0)
            foundRecords++;
        else
            notFoundRecords++;
    }

    double elapsed_time = (omp_get_wtime() - start_time) * 1000.0;

    // Check for reading errors
    if (ferror(file))
    {
        perror("Error reading file");
    }

    // Clean up
    fclose(file);
    free(buffer);

    // Print the total number of times the condition was met
    // if (foundRecord == true)
    //	printf("NONCE found (%zu) for HASH prefix %s\n",fRecord,SEARCH_STRING);
    // else
    //	printf("no NONCE found for HASH prefix %s\n",SEARCH_STRING);
    if (!BENCHMARK)
        printf("searched for %d lookups of %d bytes long, found %d, not found %d in %.2f seconds, %.2f ms per lookup\n", num_lookups, search_size, foundRecords, notFoundRecords, elapsed_time / 1000.0, elapsed_time / num_lookups);
    else
        printf("%s %d %zu %llu %llu %d %d %d %d %.2f %.2f\n", filename, NUM_THREADS, filesize, num_buckets_search, num_records_in_bucket_search, num_lookups, search_size, foundRecords, notFoundRecords, elapsed_time / 1000.0, elapsed_time / num_lookups);
    // return NULL;
}

uint64_t largest_power_of_two_less_than(uint64_t number)
{
    if (number == 0)
    {
        return 0;
    }

    // Decrement number to handle cases where number is already a power of 2
    number--;

    // Set all bits to the right of the most significant bit
    number |= number >> 1;
    number |= number >> 2;
    number |= number >> 4;
    number |= number >> 8;
    number |= number >> 16;
    number |= number >> 32; // Only needed for 64-bit integers

    // The most significant bit is now set; shift right to get the largest power of 2 less than the original number
    return (number + 1) >> 1;
}

int rename_file(const char *old_name, const char *new_name)
{
    // Attempt to rename the file
    if (rename(old_name, new_name) != 0)
    {
        // If rename fails, perror prints a descriptive error message
        perror("Error renaming file");
        return -1;
    }

    // Success
    return 0;
}

void remove_file(const char *fileName)
{
    // Attempt to remove the file
    if (remove(fileName) == 0)
    {
        if (DEBUG)
            printf("File '%s' removed successfully.\n", fileName);
    }
    else
    {
        perror("Error removing file");
    }
}

int move_file_overwrite(const char *source_path, const char *destination_path)
{
    if (DEBUG)
        printf("move_file_overwrite()...\n");
    // Attempt to rename the file
    if (rename(source_path, destination_path) == 0)
    {
        if (!BENCHMARK)
            printf("rename success!\n");
        return 0;
    }

    // If rename failed, check if it's due to cross-device move
    if (errno != EXDEV)
    {
        perror("Error renaming file");
        return -1;
    }

    // Proceed to copy and delete since it's a cross-filesystem move

    // Remove the destination file if it exists to allow overwriting
    if (remove(destination_path) != 0 && errno != ENOENT)
    {
        perror("Error removing existing destination file");
        return -1;
    }

    // Continue with copying as in the original move_file function
    FILE *source = fopen(source_path, "rb");
    if (source == NULL)
    {
        perror("Error opening source file for reading");
        return -1;
    }

    FILE *destination = fopen(destination_path, "wb");
    if (destination == NULL)
    {
        perror("Error opening destination file for writing");
        fclose(source);
        return -1;
    }

    if (!BENCHMARK)
        printf("deep copy started...\n");
    size_t buffer_size = 1024 * 1024 * 8; // 1 MB
    uint8_t *buffer = (uint8_t *)calloc(buffer_size, 1);

    if (buffer == NULL)
    {
        perror("Failed to allocate memory");
        return EXIT_FAILURE;
    }

    // char buffer[1024*1024];
    size_t bytes;

    while ((bytes = fread(buffer, 1, sizeof(buffer), source)) > 0)
    {
        size_t bytes_written = fwrite(buffer, 1, bytes, destination);
        if (bytes_written != bytes)
        {
            perror("Error writing to destination file");
            fclose(source);
            fclose(destination);
            return -1;
        }
    }

    if (ferror(source))
    {
        perror("Error reading from source file");
        fclose(source);
        fclose(destination);
        return -1;
    }

    if (fflush(source) != 0)
    {
        perror("Failed to flush buffer");
        fclose(source);
        return EXIT_FAILURE;
    }

    if (fsync(fileno(source)) != 0)
    {
        perror("Failed to fsync buffer");
        fclose(source);
        return EXIT_FAILURE;
    }

    fclose(source);

    if (fflush(destination) != 0)
    {
        perror("Failed to flush buffer");
        fclose(destination);
        return EXIT_FAILURE;
    }

    if (fsync(fileno(destination)) != 0)
    {
        perror("Failed to fsync buffer");
        fclose(destination);
        return EXIT_FAILURE;
    }

    fclose(destination);

    if (remove(source_path) != 0)
    {
        perror("Error deleting source file after moving");
        return -1;
    }

    if (!BENCHMARK)
        printf("deep copy finished!\n");
    if (DEBUG)
        printf("move_file_overwrite() finished!\n");
    return 0; // Success
}

int main(int argc, char *argv[])
{
    // Default values
    const char *approach = "for"; // Default approach
    int num_threads = 0;          // 0 means OpenMP chooses
    int num_threads_io = 0;
    int K = 4;                                     // Default exponent
    unsigned long long num_iterations = 1ULL << K; // 2^K iterations
    unsigned long long num_hashes = num_iterations;
    unsigned long long MEMORY_SIZE_MB = 1;
    // unsigned long long MEMORY_SIZE_bytes_original = 0;
    char *FILENAME = NULL;       // Default output file name
    char *FILENAME_FINAL = NULL; // Default output file name
    char *SEARCH_STRING = NULL;  // Default output file name

    // Define long options
    static struct option long_options[] = {
        {"approach", required_argument, 0, 'a'},
        {"threads", required_argument, 0, 't'},
        {"threads_io", required_argument, 0, 'i'},
        {"exponent", required_argument, 0, 'K'},
        {"memory", required_argument, 0, 'm'},
        {"file", required_argument, 0, 'f'},
        {"file_final", required_argument, 0, 'g'},
        {"batch-size", required_argument, 0, 'b'},
        {"memory_write", required_argument, 0, 'w'},
        {"circular_array", required_argument, 0, 'c'},
        {"verify", required_argument, 0, 'v'},
        {"search", required_argument, 0, 's'},
        {"prefix_search_size", required_argument, 0, 'p'},
        {"benchmark", required_argument, 0, 'x'},
        {"debug", required_argument, 0, 'd'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    // add option x for benchmark

    int opt;
    int option_index = 0;

    // Parse command-line arguments
    while ((opt = getopt_long(argc, argv, "a:t:i:K:m:f:g:b:w:c:v:s:p:x:d:h", long_options, &option_index)) != -1)
    {
        switch (opt)
        {
        case 'a':
            if (strcmp(optarg, "task") == 0 || strcmp(optarg, "for") == 0 || strcmp(optarg, "tbb") == 0)
            {
                approach = optarg;
            }
            else
            {
                fprintf(stderr, "Invalid approach: %s\n", optarg);
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 't':
            num_threads = atoi(optarg);
            if (num_threads <= 0)
            {
                fprintf(stderr, "Number of threads must be positive.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 'i':
            num_threads_io = atoi(optarg);
            if (num_threads_io <= 0)
            {
                fprintf(stderr, "Number of I/O threads must be positive.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 'K':
            K = atoi(optarg);
            if (K < 24 || K > 40)
            { // Limiting K to avoid overflow
                fprintf(stderr, "Exponent K must be between 24 and 40.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            num_iterations = 1ULL << K; // Compute 2^K
            break;
        case 'm':
            MEMORY_SIZE_MB = atoi(optarg);
            // MEMORY_SIZE_bytes_original = MEMORY_SIZE_MB * 1024 * 1024;
            if (MEMORY_SIZE_MB < 64)
            {
                fprintf(stderr, "Memory size must be at least 64 MB.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 'f':
            FILENAME = optarg;
            writeData = true;
            break;
        case 'g':
            FILENAME_FINAL = optarg;
            writeDataFinal = true;
            break;
        case 'b':
            BATCH_SIZE = atoi(optarg);
            if (BATCH_SIZE < 1)
            {
                fprintf(stderr, "BATCH_SIZE must be 1 or greater.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 'w':
            if (strcmp(optarg, "true") == 0)
            {
                MEMORY_WRITE = true;
            }
            else
            {
                MEMORY_WRITE = false;
            }
            break;
        case 'c':
            if (strcmp(optarg, "true") == 0)
            {
                CIRCULAR_ARRAY = true;
            }
            else
            {
                CIRCULAR_ARRAY = false;
            }
            break;
        case 'v':
            if (strcmp(optarg, "true") == 0)
            {
                VERIFY = true;
            }
            else
            {
                VERIFY = false;
            }
            break;
        case 's':
            SEARCH_STRING = optarg;
            SEARCH = true;
            HASHGEN = false;
            break;
        case 'p':
            SEARCH_BATCH = true;
            SEARCH = true;
            HASHGEN = false;
            PREFIX_SEARCH_SIZE = atoi(optarg);
            if (PREFIX_SEARCH_SIZE < 1)
            {
                fprintf(stderr, "PREFIX_SEARCH_SIZE must be 1 or greater.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        case 'x':
            if (strcmp(optarg, "true") == 0)
            {
                BENCHMARK = true;
            }
            else
            {
                BENCHMARK = false;
            }
            break;
        case 'd':
            if (strcmp(optarg, "true") == 0)
            {
                DEBUG = true;
            }
            else
            {
                DEBUG = false;
            }
            break;
        case 'h':
        default:
            print_usage(argv[0]);
            exit(EXIT_SUCCESS);
        }
    }

    NUM_THREADS = num_threads;
    // Set the number of threads if specified
    if (num_threads > 0)
    {
        omp_set_num_threads(num_threads);
    }

    // Display selected configurations
    if (!BENCHMARK)
    {
        if (!SEARCH)
        {
            printf("Selected Approach           : %s\n", approach);
            printf("Number of Threads           : %d\n", num_threads > 0 ? num_threads : omp_get_max_threads());
            printf("Number of Threads I/O       : %d\n", num_threads_io > 0 ? num_threads_io : omp_get_max_threads());
            printf("Exponent K                  : %d\n", K);
        }
    }

    unsigned long long file_size_bytes = num_iterations * NONCE_SIZE;
    double file_size_gb = file_size_bytes / (1024 * 1024 * 1024.0);
    unsigned long long MEMORY_SIZE_bytes = 0;

    // if (MEMORY_SIZE_MB / 1024.0 > file_size_gb) {
    if (MEMORY_SIZE_MB * 1024 * 1024 > file_size_bytes)
    {
        MEMORY_SIZE_MB = (unsigned long long)(file_size_bytes / (1024 * 1024));
        MEMORY_SIZE_bytes = file_size_bytes;
    }
    else
        MEMORY_SIZE_bytes = MEMORY_SIZE_MB * 1024 * 1024;

    // printf("Memory Size (MB)            : %llu\n", MEMORY_SIZE_MB);
    // printf("Memory Size (bytes)            : %llu\n", MEMORY_SIZE_bytes);

    rounds = ceil(file_size_bytes / MEMORY_SIZE_bytes);
    MEMORY_SIZE_bytes = file_size_bytes / rounds;
    num_hashes = floor(MEMORY_SIZE_bytes / NONCE_SIZE);
    MEMORY_SIZE_bytes = num_hashes * NONCE_SIZE;
    file_size_bytes = MEMORY_SIZE_bytes * rounds;
    file_size_gb = file_size_bytes / (1024 * 1024 * 1024.0);

    // MEMORY_SIZE_bytes

    MEMORY_SIZE_MB = (unsigned long long)(MEMORY_SIZE_bytes / (1024 * 1024));

    num_hashes = MEMORY_SIZE_bytes / NONCE_SIZE;

    num_buckets = 1ULL << (PREFIX_SIZE * 8);

    num_records_in_bucket = num_hashes / num_buckets;

    MEMORY_SIZE_bytes = num_buckets * num_records_in_bucket * sizeof(MemoRecord);
    MEMORY_SIZE_MB = (unsigned long long)(MEMORY_SIZE_bytes / (1024 * 1024));
    file_size_bytes = MEMORY_SIZE_bytes * rounds;
    file_size_gb = file_size_bytes / (1024 * 1024 * 1024.0);
    num_hashes = floor(MEMORY_SIZE_bytes / NONCE_SIZE);
    num_iterations = num_hashes * rounds;

    if (!BENCHMARK)
    {

        if (SEARCH)
        {
            printf("SEARCH                      : true\n");
            // printf("SEARCH_STRING               : %s\n",SEARCH_STRING);
        }
        else
        {
            // printf("SEARCH                      : false\n");
            printf("File Size (GB)              : %.2f\n", file_size_gb);
            printf("File Size (bytes)           : %llu\n", file_size_bytes);

            printf("Memory Size (MB)            : %llu\n", MEMORY_SIZE_MB);
            printf("Memory Size (bytes)         : %llu\n", MEMORY_SIZE_bytes);

            printf("Number of Hashes (RAM)      : %llu\n", num_hashes);

            printf("Number of Hashes (Disk)     : %llu\n", num_iterations);
            printf("Size of MemoRecord          : %lu\n", sizeof(MemoRecord));
            printf("Rounds                      : %llu\n", rounds);

            printf("Number of Buckets           : %llu\n", num_buckets);
            printf("Number of Records in Bucket : %llu\n", num_records_in_bucket);

            printf("BATCH_SIZE                  : %zu\n", BATCH_SIZE);

            if (HASHGEN)
                printf("HASHGEN                     : true\n");
            else
                printf("HASHGEN                     : false\n");

            if (MEMORY_WRITE)
                printf("MEMORY_WRITE                : true\n");
            else
                printf("MEMORY_WRITE                : false\n");

            if (CIRCULAR_ARRAY)
                printf("CIRCULAR_ARRAY              : true\n");
            else
                printf("CIRCULAR_ARRAY              : false\n");

            if (writeData)
            {
                printf("Temporary File              : %s\n", FILENAME);
            }
            if (writeDataFinal)
            {
                printf("Output File Final           : %s\n", FILENAME_FINAL);
            }
        }
    }

    if (HASHGEN)
    {
        printf("HASHGEN                      : true\n");

        // Open the file for writing in binary mode
        FILE *fd = NULL;
        if (writeData)
        {
            fd = fopen(FILENAME, "wb+");
            if (fd == NULL)
            {
                printf("Error opening file %s (#4)\n", FILENAME);

                perror("Error opening file");
                return EXIT_FAILURE;
            }
        }

        // Start walltime measurement
        double start_time = omp_get_wtime();

        // Allocate memory for the array of Buckets
        Bucket *buckets = (Bucket *)calloc(num_buckets, sizeof(Bucket));
        if (buckets == NULL)
        {
            fprintf(stderr, "Error: Unable to allocate memory for buckets.\n");
            exit(EXIT_FAILURE);
        }

        // Allocate memory for each bucket's records
        for (unsigned long long i = 0; i < num_buckets; i++)
        {
            buckets[i].records = (MemoRecord *)calloc(num_records_in_bucket, sizeof(MemoRecord));
            if (buckets[i].records == NULL)
            {
                fprintf(stderr, "Error: Unable to allocate memory for records.\n");
                exit(EXIT_FAILURE);
            }
        }

        double throughput_hash = 0.0;
        double throughput_io = 0.0;

        double start_time_io = 0.0;
        double end_time_io = 0.0;
        double elapsed_time_io = 0.0;
        double elapsed_time_io2 = 0.0;
        double start_time_hash = 0.0;
        double end_time_hash = 0.0;
        double elapsed_time_hash = 0.0;

        double elapsed_time_hash_total = 0.0;
        double elapsed_time_io_total = 0.0;
        double elapsed_time_io2_total = 0.0;

        for (unsigned long long r = 0; r < rounds; r++)
        {
            start_time_hash = omp_get_wtime();

            // Reset bucket counts
            for (unsigned long long i = 0; i < num_buckets; i++)
            {
                buckets[i].count = 0;
                buckets[i].flush = 0;
            }

            unsigned long long start_idx = r * num_hashes;
            unsigned long long end_idx = start_idx + num_hashes;

            // Parallel region based on selected approach
            if (strcmp(approach, "task") == 0)
            {
// Tasking Model Approach
#pragma omp parallel
                {
#pragma omp single nowait
                    {
                        for (unsigned long long i = start_idx; i < end_idx; i += BATCH_SIZE)
                        {
#pragma omp task
                            {
                                MemoRecord record;
                                uint8_t record_hash[HASH_SIZE];

                                unsigned long long batch_end = i + BATCH_SIZE;
                                if (batch_end > end_idx)
                                {
                                    batch_end = end_idx;
                                }

                                for (unsigned long long j = i; j < batch_end; j++)
                                {
                                    generateBlake3(record_hash, &record, j);
                                    if (MEMORY_WRITE)
                                    {
                                        off_t bucketIndex = getBucketIndex(record_hash, PREFIX_SIZE);
                                        insert_record(buckets, &record, bucketIndex);
                                    }
                                }
                            }
                        }
                    }
                } // Implicit barrier ensures all tasks are completed before exiting
            }
            else if (strcmp(approach, "for") == 0)
            {
// Parallel For Loop Approach
// #pragma omp parallel for schedule(dynamic)
#pragma omp parallel for schedule(static)
                for (unsigned long long i = start_idx; i < end_idx; i += BATCH_SIZE)
                {
                    MemoRecord record;
                    uint8_t record_hash[HASH_SIZE];

                    unsigned long long batch_end = i + BATCH_SIZE;
                    if (batch_end > end_idx)
                    {
                        batch_end = end_idx;
                    }

                    for (unsigned long long j = i; j < batch_end; j++)
                    {
                        generateBlake3(record_hash, &record, j);
                        if (MEMORY_WRITE)
                        {
                            off_t bucketIndex = getBucketIndex(record_hash, PREFIX_SIZE);
                            insert_record(buckets, &record, bucketIndex);
                        }
                    }
                }
            }
#ifndef __cplusplus
            // Your C-specific code here
            else if (strcmp(approach, "tbb") == 0)
            {
                printf("TBB is not supported with C, use C++ compiler instead to build vaultx, exiting...\n");
                exit(1);
            }
#endif

#ifdef __cplusplus
            // Your C++-specific code here

            else if (strcmp(approach, "tbb") == 0)
            {
                // Parallel For Loop Approach
                tbb::parallel_for(
                    tbb::blocked_range<unsigned long long>(start_idx, end_idx, BATCH_SIZE),
                    [&](const tbb::blocked_range<unsigned long long> &batch_range)
                    {
                        // Process each batch in the range
                        for (unsigned long long i = batch_range.begin(); i < batch_range.end(); i += BATCH_SIZE)
                        {
                            unsigned long long batch_end = i + BATCH_SIZE;
                            if (batch_end > end_idx)
                            {
                                batch_end = end_idx;
                            }

                            for (unsigned long long j = i; j < batch_end; ++j)
                            {
                                MemoRecord record;
                                uint8_t record_hash[HASH_SIZE];

                                generateBlake3(record_hash, &record, j);

                                if (MEMORY_WRITE)
                                {
                                    off_t bucketIndex = getBucketIndex(record_hash, PREFIX_SIZE);
                                    insert_record(buckets, &record, bucketIndex);
                                }
                            }
                        }
                    });
            }
#endif

            // after else if

            // End hash computation time measurement
            end_time_hash = omp_get_wtime();
            elapsed_time_hash = end_time_hash - start_time_hash;
            elapsed_time_hash_total += elapsed_time_hash;

            // Write data to disk if required
            if (writeData)
            {
                start_time_io = omp_get_wtime();

                // Seek to the correct position in the file
                off_t offset = r * num_records_in_bucket * num_buckets * NONCE_SIZE;
                if (fseeko(fd, offset, SEEK_SET) < 0)
                {
                    perror("Error seeking in file");
                    fclose(fd);
                    exit(EXIT_FAILURE);
                }

                size_t bytesWritten = 0;
                // Write buckets to disk
                for (unsigned long long i = 0; i < num_buckets; i++)
                {
                    bytesWritten += writeBucketToDiskSequential(&buckets[i], fd);
                    // printf("writeBucketToDiskSequential(): %llu bytes\n",bytesWritten);
                }

                // printf("writeBucketToDiskSequential(): %llu bytes at offset %llu; num_hashes=%llu\n",bytesWritten,offset,num_hashes);

                // End I/O time measurement
                end_time_io = omp_get_wtime();
                elapsed_time_io = end_time_io - start_time_io;
                elapsed_time_io_total += elapsed_time_io;

                // printf("%.2f MB/s\n", throughput_io);
            }

            // Calculate throughput (hashes per second)
            throughput_hash = (num_hashes / (elapsed_time_hash + elapsed_time_io)) / (1e6);

            // Calculate I/O throughput
            throughput_io = (num_hashes * NONCE_SIZE) / ((elapsed_time_hash + elapsed_time_io) * 1024 * 1024);

            if (!BENCHMARK)
                printf("[%.2f] HashGen %.2f%%: %.2f MH/s : I/O %.2f MB/s\n", omp_get_wtime() - start_time, (r + 1) * 100.0 / rounds, throughput_hash, throughput_io);
            // end of loop
            // else {
            //    printf("\n");
            //}
        }

        start_time_io = omp_get_wtime();

        // Flush and close the file
        if (writeData)
        {
            if (fflush(fd) != 0)
            {
                perror("Failed to flush buffer");
                fclose(fd);
                return EXIT_FAILURE;
            }
            // fclose(fd);
        }

        end_time_io = omp_get_wtime();
        elapsed_time_io = end_time_io - start_time_io;
        elapsed_time_io_total += elapsed_time_io;

        // should move timing for I/O to after this section of code

        /*
        // Verification if enabled
        if (VERIFY) {
            unsigned long long num_zero = 0;
            for (unsigned long long i = 0; i < num_buckets; i++) {
                for (unsigned long long j = 0; j < buckets[i].count; j++) {
                    if (byteArrayToLongLong(buckets[i].records[j].nonce, NONCE_SIZE) == 0)
                        num_zero++;
                }
            }
            printf("Number of zero nonces: %llu\n", num_zero);
        }*/

        // Free allocated memory
        for (unsigned long long i = 0; i < num_buckets; i++)
        {
            free(buckets[i].records);
        }
        free(buckets);

        if (writeDataFinal && rounds > 1)
        {
            // Open the file for writing in binary mode
            FILE *fd_dest = NULL;
            if (writeDataFinal)
            {
                fd_dest = fopen(FILENAME_FINAL, "wb+");
                if (fd_dest == NULL)
                {
                    printf("Error opening file %s (#5)\n", FILENAME_FINAL);
                    perror("Error opening file");

                    // perror("Error opening file");
                    return EXIT_FAILURE;
                }
            }

            unsigned long long num_buckets_to_read = ceil((MEMORY_SIZE_bytes / (num_records_in_bucket * rounds * NONCE_SIZE)) / 2);
            if (DEBUG)
                printf("will read %llu buckets at one time, %llu bytes\n", num_buckets_to_read, num_records_in_bucket * rounds * NONCE_SIZE * num_buckets_to_read);
            // need to fix this for 5 byte NONCE_SIZE
            if (num_buckets % num_buckets_to_read != 0)
            {
                uint64_t ratio = num_buckets / num_buckets_to_read;
                uint64_t result = largest_power_of_two_less_than(ratio);
                if (DEBUG)
                    printf("Largest power of 2 less than %lu is %lu\n", ratio, result);
                num_buckets_to_read = num_buckets / result;
                if (DEBUG)
                    printf("will read %llu buckets at one time, %llu bytes\n", num_buckets_to_read, num_records_in_bucket * rounds * NONCE_SIZE * num_buckets_to_read);
                // printf("error, num_buckets_to_read is not a multiple of num_buckets, exiting: num_buckets=%llu num_buckets_to_read=%llu...\n",num_buckets,num_buckets_to_read);
                // return EXIT_FAILURE;
            }

            // Calculate the total number of records to read per batch
            size_t records_per_batch = num_records_in_bucket * num_buckets_to_read;
            // Calculate the size of the buffer needed
            size_t buffer_size = records_per_batch * rounds;
            // Allocate the buffer
            if (DEBUG)
                printf("allocating %lu bytes for buffer\n", buffer_size * sizeof(MemoRecord));
            MemoRecord *buffer = (MemoRecord *)malloc(buffer_size * sizeof(MemoRecord));
            if (buffer == NULL)
            {
                fprintf(stderr, "Error allocating memory for buffer.\n");
                exit(EXIT_FAILURE);
            }

            if (DEBUG)
                printf("allocating %lu bytes for bufferShuffled\n", buffer_size * sizeof(MemoRecord));
            MemoRecord *bufferShuffled = (MemoRecord *)malloc(buffer_size * sizeof(MemoRecord));
            if (bufferShuffled == NULL)
            {
                fprintf(stderr, "Error allocating memory for bufferShuffled.\n");
                exit(EXIT_FAILURE);
            }

            // Set the number of threads if specified
            if (num_threads_io > 0)
            {
                omp_set_num_threads(num_threads_io);
            }

            for (unsigned long long i = 0; i < num_buckets; i = i + num_buckets_to_read)
            {
                double start_time_io2 = omp_get_wtime();

#pragma omp parallel for schedule(static)
                for (unsigned long long r = 0; r < rounds; r++)
                {
                    // off_t offset_src_old = r * num_records_in_bucket * num_buckets * NONCE_SIZE + i*num_records_in_bucket*NONCE_SIZE;
                    //  Calculate the source offset
                    off_t offset_src = ((r * num_buckets + i) * num_records_in_bucket) * sizeof(MemoRecord);
                    // printf("read data: offset_src_old=%llu offset_src=%llu\n",offset_src_old,offset_src);
                    // if (DEBUG) printf("read data: offset_src_old=%llu bytes=%llu\n",offset_src_old,num_records_in_bucket*NONCE_SIZE*num_buckets_to_read);
                    if (DEBUG)
                        printf("read data: offset_src=%lu bytes=%lu\n",
                               offset_src, records_per_batch * sizeof(MemoRecord));

                    if (fseeko(fd, offset_src, SEEK_SET) < 0)
                    {
                        perror("Error seeking in file");
                        fclose(fd);
                        exit(EXIT_FAILURE);
                    }

                    // size_t recordsRead = fread(buffer+num_records_in_bucket*num_buckets_to_read*sizeof(MemoRecord)*r, sizeof(MemoRecord), num_records_in_bucket*num_buckets_to_read, fd);
                    //  Correct pointer arithmetic

                    size_t index = r * records_per_batch;
                    if (DEBUG)
                        printf("storing read data at index %lu\n", index);
                    size_t recordsRead = fread(&buffer[index],
                                               sizeof(MemoRecord),
                                               records_per_batch,
                                               fd);
                    // size_t recordsRead = fread(buffer + r * records_per_batch,
                    //                        sizeof(MemoRecord),
                    //                        records_per_batch,
                    //                        fd);
                    if (recordsRead != records_per_batch)
                    {
                        fprintf(stderr, "Error reading file, records read %zu instead of %zu\n",
                                recordsRead, records_per_batch);
                        fclose(fd);
                        exit(EXIT_FAILURE);
                    }
                    else
                    {
                        if (DEBUG)
                            printf("read %zu records from disk...\n", recordsRead);
                    }

                    off_t offset_dest = i * num_records_in_bucket * NONCE_SIZE * rounds;
                    if (DEBUG)
                        printf("write data: offset_dest=%lu bytes=%llu\n", offset_dest, num_records_in_bucket * NONCE_SIZE * rounds * num_buckets_to_read);

                    if (fseeko(fd_dest, offset_dest, SEEK_SET) < 0)
                    {
                        perror("Error seeking in file");
                        fclose(fd_dest);
                        exit(EXIT_FAILURE);
                    }
                    // needs to make sure its ok, fix things....
                    // printf("buffer_size=%llu my_buffer_size=%llu\n",buffer_size,num_records_in_bucket*num_buckets_to_read*rounds);
                }
                // end of for loop rounds

                if (DEBUG)
                    printf("shuffling %llu buckets with %llu bytes each...\n", num_buckets_to_read * rounds, num_records_in_bucket * NONCE_SIZE);
#pragma omp parallel for schedule(static)
                for (unsigned long long s = 0; s < num_buckets_to_read; s++)
                {
                    for (unsigned long long r = 0; r < rounds; r++)
                    {

                        // off_t index_src = s*num_records_in_bucket+r*num_records_in_bucket*rounds;
                        // off_t index_dest = s + r*num_records_in_bucket*rounds;

                        off_t index_src = ((r * num_buckets_to_read + s) * num_records_in_bucket);
                        off_t index_dest = (s * rounds + r) * num_records_in_bucket;

                        // printf("SHUFFLE: index_src=%llu index_dest=%llu\n",index_src,index_dest);
                        // printf("SHUFFLE: s=%llu, r=%llu, index_src=%llu, index_dest=%llu\n", s, r, index_src, index_dest);

                        memcpy(&bufferShuffled[index_dest], &buffer[index_src], num_records_in_bucket * sizeof(MemoRecord));
                    }
                }
                // end of for loop num_buckets_to_read

                // should write in parallel if possible
                size_t elementsWritten = fwrite(bufferShuffled, sizeof(MemoRecord), num_records_in_bucket * num_buckets_to_read * rounds, fd_dest);
                if (elementsWritten != num_records_in_bucket * num_buckets_to_read * rounds)
                {
                    fprintf(stderr, "Error writing bucket to file; elements written %zu when expected %llu\n",
                            elementsWritten, num_records_in_bucket * num_buckets_to_read * rounds);
                    fclose(fd_dest);
                    exit(EXIT_FAILURE);
                }

                /*if (fsync(fileno(fd_dest)) != 0) {
                    perror("Failed to fsync buffer");
                    fclose(fd_dest);
                    return EXIT_FAILURE;
                }*/

                double end_time_io2 = omp_get_wtime();
                elapsed_time_io2 = end_time_io2 - start_time_io2;
                elapsed_time_io2_total += elapsed_time_io2;
                double throughput_io2 = (num_records_in_bucket * num_buckets_to_read * rounds * NONCE_SIZE) / (elapsed_time_io2 * 1024 * 1024);
                if (!BENCHMARK)
                    printf("[%.2f] Shuffle %.2f%%: %.2f MB/s\n", omp_get_wtime() - start_time, (i + 1) * 100.0 / num_buckets, throughput_io2);
            }
            // end of for loop
            start_time_io = omp_get_wtime();

            // Flush and close the file
            if (writeData)
            {
                if (fflush(fd) != 0)
                {
                    perror("Failed to flush buffer");
                    fclose(fd);
                    return EXIT_FAILURE;
                }

                if (fsync(fileno(fd)) != 0)
                {
                    perror("Failed to fsync buffer");
                    fclose(fd);
                    return EXIT_FAILURE;
                }
                fclose(fd);
            }

            if (writeDataFinal)
            {
                if (fflush(fd_dest) != 0)
                {
                    perror("Failed to flush buffer");
                    fclose(fd_dest);
                    return EXIT_FAILURE;
                }

                if (fsync(fileno(fd_dest)) != 0)
                {
                    perror("Failed to fsync buffer");
                    fclose(fd_dest);
                    return EXIT_FAILURE;
                }

                fclose(fd_dest);

                remove_file(FILENAME);
            }

            free(buffer);
        }
        else if (writeDataFinal && rounds == 1)
        {
            // Call the rename_file function
            if (move_file_overwrite(FILENAME, FILENAME_FINAL) == 0)
            {
                if (!BENCHMARK)
                    printf("File renamed/moved successfully from '%s' to '%s'.\n", FILENAME, FILENAME_FINAL);
            }
            else
            {
                printf("Error in moving file '%s' to '%s'.\n", FILENAME, FILENAME_FINAL);
                return EXIT_FAILURE;
                // Error message already printed by rename_file via perror()
                // Additional handling can be done here if necessary
                // return 1;
            }
        }

// will need to check on MacOS with a spinning hdd if we need to call sync() to flush all filesystems
#ifdef __linux__
        if (DEBUG)
            printf("Final flush in progress...\n");
        int fd2 = open(FILENAME_FINAL, O_RDWR);
        if (fd2 == -1)
        {
            printf("Error opening file %s (#6)\n", FILENAME_FINAL);

            perror("Error opening file");
            return EXIT_FAILURE;
        }

        // Sync the entire filesystem
        if (syncfs(fd2) == -1)
        {
            perror("Error syncing filesystem with syncfs");
            close(fd2);
            return EXIT_FAILURE;
        }
#endif

        end_time_io = omp_get_wtime();
        elapsed_time_io = end_time_io - start_time_io;
        elapsed_time_io_total += elapsed_time_io;

        // End total time measurement
        double end_time = omp_get_wtime();
        double elapsed_time = end_time - start_time;

        // Calculate total throughput
        double total_throughput = (num_iterations / elapsed_time) / 1e6;
        if (!BENCHMARK)
        {
            printf("Total Throughput: %.2f MH/s  %.2f MB/s\n", total_throughput, total_throughput * NONCE_SIZE);
            printf("Total Time: %.6f seconds\n", elapsed_time);
        }
        else
        {
            printf("%s %d %lu %d %llu %.2f %zu %.2f %.2f %.2f %.2f %.2f %.2f %.2f\n", approach, K, sizeof(MemoRecord), num_threads, MEMORY_SIZE_MB, file_size_gb, BATCH_SIZE, total_throughput, total_throughput * NONCE_SIZE, elapsed_time_hash_total, elapsed_time_io_total, elapsed_time_io2_total, elapsed_time - elapsed_time_hash_total - elapsed_time_io_total - elapsed_time_io2_total, elapsed_time);
            return 0;
        }
    }
    // end of HASHGEN

    omp_set_num_threads(num_threads);

    if (SEARCH && !SEARCH_BATCH)
    {
        // printf("search has not been implemented yet...\n");
        search_memo_records(FILENAME_FINAL, SEARCH_STRING);
    }

    if (SEARCH_BATCH)
    {
        // printf("search has not been implemented yet...\n");
        search_memo_records_batch(FILENAME_FINAL, BATCH_SIZE, PREFIX_SEARCH_SIZE);
    }

    // Call the function to count zero-value MemoRecords
    // printf("verifying efficiency of final stored file...\n");
    // count_zero_memo_records(FILENAME_FINAL);

    // Call the function to process MemoRecords
    if (VERIFY)
    {
        if (!BENCHMARK)
            printf("verifying sorted order by bucketIndex of final stored file...\n");
        process_memo_records(FILENAME_FINAL, MEMORY_SIZE_bytes / sizeof(MemoRecord));
    }

    if (DEBUG)
        printf("SUCCESS!\n");
    return 0;
}
