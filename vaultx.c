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
bool LOOKUP = false;
bool LOOKUP_BATCH = false;
bool PRINT = false;
bool writeData = false;
bool writeDataFinal = false;

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

// Function to convert bytes to unsigned long long
// Also used to get bucket index
unsigned long long byteArrayToLongLong(const uint8_t *byteArray, size_t length)
{
    unsigned long long result = 0;
    for (size_t i = 0; i < length; ++i)
    {
        result = (result << 8) | (unsigned long long)byteArray[i];
    }
    return result;
}

// Function to convert a hexadecimal string to a byte array
int hex_to_bytes(const char *hex_string, uint8_t *byte_array, size_t byte_array_size)
{
    for (size_t i = 0; i < byte_array_size; ++i)
    {
        // Convert each pair of hex characters to a single byte
        if (sscanf(&hex_string[i * 2], "%2hhx", &byte_array[i]) != 1)
        {
            fprintf(stderr, "Error: Invalid hex string format.\n");
            return -1;
        }
    }
    return 0; // Success
}

// Function to generate Blake3 hash
void generateBlake3(uint8_t *record_hash, MemoRecord *record, unsigned long long *seed, size_t hash_length)
{
    // Ensure that the pointers are valid
    if (record_hash == NULL || record == NULL)
    {
        fprintf(stderr, "Error: NULL pointer passed to generateBlake3.\n");
        return;
    }

    if (seed != NULL)
    {
        // Store seed into the nonce
        memcpy(record->nonce, seed, NONCE_SIZE);
    }

    if (hash_length == 0)
    {
        hash_length = HASH_SIZE;
    }

    // Generate Blake3 hash
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, record->nonce, NONCE_SIZE);
    blake3_hasher_finalize(&hasher, record_hash, hash_length);
}

// Function to write a bucket of records to disk sequentially
size_t writeBucketToDiskSequential(const Bucket *bucket, FILE *fd)
{
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
    char *result = malloc(len1 + len2 + 1);
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
        printf("Size of '%s' is %ld bytes.\n", filename, filesize);
    }

    // Open the file for reading in binary mode
    file = fopen(filename, "rb");
    if (file == NULL)
    {
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
    printf("sorted=%zu not_sorted=%zu zero_nonces=%zu total_records=%zu\n",
           count_condition_met, count_condition_not_met, zero_nonce_count, total_records);

    return count_condition_met;
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
    // Attempt to rename the file
    if (rename(source_path, destination_path) == 0)
    {
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

    printf("deep copy started...\n");
    size_t buffer_size = 1024 * 1024 * 8; // 1 MB
    uint8_t *buffer = calloc(buffer_size, 1);

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

    fclose(source);
    fclose(destination);

    if (remove(source_path) != 0)
    {
        perror("Error deleting source file after moving");
        return -1;
    }

    printf("deep copy finished!\n");
    return 0; // Success
}

// Function to read and print specified number of hashes from a file
void print_hashes_from_file(const char *filename, size_t num_hashes)
{
    FILE *file = fopen(filename, "rb"); // Open the file in binary read mode
    if (file == NULL)
    {
        perror("Error opening file");
        return;
    }

    MemoRecord record;
    uint8_t generated_hash[28];
    size_t hashes_printed = 0;

    // Read nonces from the file and generate hashes
    while (fread(record.nonce, 1, NONCE_SIZE, file) == NONCE_SIZE && hashes_printed < num_hashes)
    {
        // Generate hash for the nonce using blake3
        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, record.nonce, NONCE_SIZE);
        blake3_hasher_finalize(&hasher, generated_hash, 28);

        for (size_t i = 0; i < 28; ++i)
        {
            printf("%02x", generated_hash[i]);
        }
        printf("\n");

        hashes_printed++;
    }

    if (hashes_printed == 0)
    {
        printf("No nonces found or the file is too small to read any full nonces.\n");
    }

    fclose(file); // Close the file
}

int load_config(const char *config_filename,
                char *approach,
                int *K,
                unsigned long long *num_buckets,
                unsigned long long *bucket_size,
                int *prefix_size,
                int *nonce_size)
{
    FILE *config_file = fopen(config_filename, "r");
    if (config_file == NULL)
    {
        perror("Error opening config file");
        return -1;
    }

    char line[256];
    while (fgets(line, sizeof(line), config_file))
    {
        if (strncmp(line, "APPROACH=", 9) == 0)
        {
            if (approach != NULL)
            {
                if (line[9] == 'f')
                {                                   // If the first character is 'f', it's "for"
                    strncpy(approach, line + 9, 3); // Copy "for"
                    approach[3] = '\0';             // Ensure null-termination
                }
                else if (line[9] == 't')
                {                                   // If the first character is 't', it's "task"
                    strncpy(approach, line + 9, 4); // Copy "task"
                    approach[4] = '\0';             // Ensure null-termination
                }
            }
        }
        else if (strncmp(line, "K=", 2) == 0)
        {
            *K = atoi(line + 12);
        }
        else if (strncmp(line, "NUM_BUCKETS=", 12) == 0)
        {
            *num_buckets = strtoull(line + 12, NULL, 10);
        }
        else if (strncmp(line, "BUCKET_SIZE=", 12) == 0)
        {
            *bucket_size = strtoull(line + 12, NULL, 10);
        }
        else if (strncmp(line, "PREFIX_SIZE=", 12) == 0)
        {
            *prefix_size = atoi(line + 12);
        }
        else if (strncmp(line, "NONCE_SIZE=", 11) == 0)
        {
            *nonce_size = atoi(line + 11);
        }
    }

    fclose(config_file);
    return 0;
}

int lookup_hash(FILE *data_file, const uint8_t *target_hash, const size_t target_hash_size, const unsigned long long num_buckets, const unsigned long long bucket_size, const int prefix_size, const int nonce_size)
{
    // Calculate the bucket index for the target hash
    off_t bucket_index = byteArrayToLongLong(target_hash, prefix_size); // Function extracts the prefix_size from hash and converts to long long to get the index of bucket

    if (DEBUG)
        printf("Computed bucket index: %ld\n", bucket_index);

    if (bucket_index >= (off_t)num_buckets)
    {
        fprintf(stderr, "Error: Computed bucket index %ld is out of range (0 to %llu).\n", bucket_index, num_buckets - 1);
        return -1;
    }

    // Calculate the exact offset in the file for this bucket
    unsigned long long bucket_offset = bucket_index * bucket_size * nonce_size;

    if (DEBUG)
        printf("Calculated bucket offset: %llu\n", bucket_offset);

    // Move file pointer to the start of the required bucket
    if (fseek(data_file, bucket_offset, SEEK_SET) != 0)
    {
        perror("Error seeking to bucket offset");
        fclose(data_file);
        return -1;
    }

    Bucket bucket;
    bucket.count = bucket_size;
    bucket.records = malloc(bucket_size * sizeof(MemoRecord));
    if (bucket.records == NULL)
    {
        fprintf(stderr, "Error: Unable to allocate memory for bucket records.\n");
        fclose(data_file);
        return -1;
    }

    // Read the entire bucket into memory
    if (fread(bucket.records, sizeof(MemoRecord), bucket_size, data_file) != bucket_size)
    {
        perror("Error reading bucket data");
        free(bucket.records);
        fclose(data_file);
        return -1;
    }

    // Iterate through records in the bucket in memory
    uint8_t generated_hash[target_hash_size];
    int found = 0;

#pragma omp parallel for schedule(dynamic) shared(found) private(generated_hash)
    for (size_t record_index = 0; record_index < bucket_size; record_index++)
    {
        // Check if another thread has already found the hash
        if (found)
            continue;

        // Generate the hash for the current nonce using generateBlake3
        generateBlake3(generated_hash, &bucket.records[record_index], NULL, target_hash_size);

        // Debugging: Print the generated hash for comparison
        if (DEBUG)
        {
            printf("Generated hash: ");
            for (size_t i = 0; i < target_hash_size; i++)
            {
                printf("%02x", generated_hash[i]);
            }
            printf("\n");
        }

        // Check if the generated hash matches the target hash
        if (memcmp(generated_hash, target_hash, target_hash_size) == 0)
        {
#pragma omp critical
            {
                if (!found)
                {
                    found = 1;
                    printf("Hash found in bucket %ld, record %lu.\n", bucket_index, record_index);
                }
            }
        }
    }

    free(bucket.records);

    if (!found)
    {
        printf("Hash not found in the computed bucket.\n");
    }

    return found ? 0 : 1; // 0 if hash is found; 1 if hash is not found
}

int batch_lookup_hashes(const char *config_filename, FILE *data_file, const size_t number_lookups, const size_t hash_length)
{
    // Variables to hold configuration values
    unsigned long long num_buckets, bucket_size;
    int K, prefix_size, nonce_size;

    // Load the configuration from file
    if (load_config(config_filename, NULL, &K, &num_buckets, &bucket_size, &prefix_size, &nonce_size) != 0)
    {
        fprintf(stderr, "Failed to load configuration.\n");
        return -1;
    }

    // Seed the random number generator
    srand(time(NULL));

    char lookup_times_filename[50]; // Buffer to hold the generated filename

    // Create the filename with 'k' included
    sprintf(lookup_times_filename, "lookup_times%d.csv", K);

    FILE *lookup_times_file = fopen(lookup_times_filename, "a");
    if (lookup_times_file == NULL)
    {
        perror("Error opening lookup times file");
        return -1;
    }

    for (size_t i = 0; i < number_lookups; i++)
    {
        MemoRecord record;
        for (size_t j = 0; j < sizeof(record.nonce); j++)
        {
            record.nonce[j] = rand() % 256;
        }

        // Generate a hash of the specified length from the nonce
        uint8_t target_hash[hash_length];
        generateBlake3(target_hash, &record, NULL, hash_length);

        // Debugging: Print the generated hash for comparison
        if (DEBUG)
        {
            printf("Generated hash for nonce %u: ", record.nonce[0]);
            printf("Generated hash: ");
            for (size_t i = 0; i < hash_length; i++)
            {
                printf("%02x", target_hash[i]);
            }
            printf("\n");
        }

        double start_lookup_time = omp_get_wtime();

        if (lookup_hash(data_file, target_hash, hash_length, num_buckets, bucket_size, prefix_size, nonce_size) == -1)
        {
            fprintf(stderr, "Error: Lookup failed for hash with length %zu.\n", hash_length);
            for (size_t i = 0; i < hash_length; i++)
            {
                printf("%02x", target_hash[i]);
            }
            printf("\n");
            return -1;
        }

        double end_lookup_time = omp_get_wtime();
        double elapsed_lookup_time = end_lookup_time - start_lookup_time;

        fprintf(lookup_times_file, "%lu,%f\n", hash_length, elapsed_lookup_time);
    }

    return 0;
}

int main(int argc, char *argv[])
{
    // Default values
    char *approach = "for";                        // Default approach
    int num_threads = 0;                           // 0 means OpenMP chooses
    int K = 4;                                     // Default exponent
    unsigned long long num_iterations = 1ULL << K; // 2^K iterations
    unsigned long long num_hashes = num_iterations;
    unsigned long long MEMORY_SIZE_MB = 1;
    char *FILENAME = NULL;       // Default output file name
    char *FILENAME_FINAL = NULL; // Default output file name
    int num_records_to_print = 0;
    int num_lookups = 0;
    int hash_length = 0;

    char *target_hash_hex = NULL; // To hold the user input as a string
    uint8_t *target_hash_bytes;   // To hold the converted binary hash
    size_t target_hash_size = 0;  // To hold the size of the binary hash

    // Define long options
    static struct option long_options[] = {
        {"approach", required_argument, 0, 'a'},
        {"threads", required_argument, 0, 't'},
        {"exponent", required_argument, 0, 'K'},
        {"memory", required_argument, 0, 'm'},
        {"file", required_argument, 0, 'f'},
        {"file_final", required_argument, 0, 'g'},
        {"batch_size", required_argument, 0, 'b'},
        {"lookup", required_argument, 0, 'l'},
        {"lookup_batch", required_argument, 0, 'L'},
        {"verify", required_argument, 0, 'v'},
        {"print", required_argument, 0, 'p'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    int option_index = 0;

    // Parse command-line arguments
    while ((opt = getopt_long(argc, argv, "a:t:K:m:f:g:b:l:L:v:p:h:", long_options, &option_index)) != -1)
    {
        switch (opt)
        {
        case 'a':
            if (strcmp(optarg, "task") == 0 || strcmp(optarg, "for") == 0)
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
            // MEMORY_SIZE_bytes_original = MEMORY_SIZE_MB*1024*1024;
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
        case 'l':
            if (optarg == NULL)
            {
                fprintf(stderr, "Error: -l flag requires a hash argument.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            target_hash_hex = strdup(optarg);
            if (target_hash_hex == NULL)
            {
                fprintf(stderr, "Memory allocation for target_hash_hex failed.\n");
                exit(EXIT_FAILURE);
            }
            target_hash_size = strlen(target_hash_hex) / 2;
            target_hash_bytes = (uint8_t *)malloc(target_hash_size);
            if (target_hash_bytes == NULL)
            {
                fprintf(stderr, "Memory allocation failed.\n");
                return -1;
            }

            // Convert the hex string to binary byte array
            if (hex_to_bytes(target_hash_hex, target_hash_bytes, target_hash_size) != 0)
            {
                fprintf(stderr, "Error converting hex string to bytes.\n");
                free(target_hash_hex);
                exit(EXIT_FAILURE);
            }
            LOOKUP = true;
            break;
        case 'L':
            // Ensure the `optarg` has a comma (delimiter) for two arguments
            if (optarg == NULL || strchr(optarg, ',') == NULL)
            {
                fprintf(stderr, "Error: -L flag requires two arguments separated by a comma (e.g., -L 100,200).\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }

            // Split `optarg` by comma
            char *first_arg = strtok(optarg, ",");
            char *second_arg = strtok(NULL, ",");

            // Convert the split arguments to integers
            num_lookups = atoi(first_arg);
            hash_length = atoi(second_arg);

            if (num_lookups <= 0 || hash_length <= 0)
            {
                fprintf(stderr, "Error: Both values for -L must be positive integers.\n");
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }

            LOOKUP_BATCH = true;
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
        case 'p':
            num_records_to_print = atoi(optarg);
            PRINT = true;
            break;
        case 'h':
        default:
            print_usage(argv[0]);
            exit(EXIT_SUCCESS);
        }
    }

    // Set the number of threads if specified
    if (num_threads > 0)
    {
        omp_set_num_threads(num_threads);
    }

    if (LOOKUP && FILENAME_FINAL != NULL)
    {
        // Variables to hold configuration values
        unsigned long long num_buckets, bucket_size;
        int K, prefix_size, nonce_size;

        char *config_filename = concat_strings(FILENAME_FINAL, ".config");
        printf("Starting lookup for hash: %s\n", target_hash_hex);

        // Open the data file containing nonces
        FILE *data_file = fopen(FILENAME_FINAL, "rb");
        if (data_file == NULL)
        {
            perror("Error opening data file");
            return -1;
        }

        // Load the configuration from file
        if (load_config(config_filename, NULL, &K, &num_buckets, &bucket_size, &prefix_size, &nonce_size) != 0)
        {
            fprintf(stderr, "Failed to load configuration.\n");
            return -1;
        }

        double start_lookup_time = omp_get_wtime();
        lookup_hash(data_file, target_hash_bytes, target_hash_size, num_buckets, bucket_size, prefix_size, nonce_size);
        double end_lookup_time = omp_get_wtime();
        double elapsed_lookup_time = end_lookup_time - start_lookup_time;
        printf("Lookup time: %.2f seconds\n", elapsed_lookup_time);

        fclose(data_file);

        free(config_filename);
        free(target_hash_hex);
        free(target_hash_bytes);

        return 0;
    }

    if (LOOKUP_BATCH && FILENAME_FINAL != NULL)
    {
        char *config_filename = concat_strings(FILENAME_FINAL, ".config");
        printf("Starting batch lookup for %d hashes\n", num_lookups);

        // Open the data file containing nonces
        FILE *data_file = fopen(FILENAME_FINAL, "rb");
        if (data_file == NULL)
        {
            perror("Error opening data file");
            return -1;
        }

        batch_lookup_hashes(config_filename, data_file, num_lookups, hash_length);

        free(config_filename);
        fclose(data_file);

        return 0;
    }

    if (PRINT && FILENAME_FINAL != NULL)
    {
        print_hashes_from_file(FILENAME_FINAL, num_records_to_print);
        return 0;
    }

    // Display selected configurations
    printf("Selected Approach           : %s\n", approach);
    printf("Number of Threads           : %d\n", num_threads > 0 ? num_threads : omp_get_max_threads());
    printf("Exponent K                  : %d\n", K);

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

    if (writeData || writeDataFinal)
    {
        const char *EXTENSION = ".config";
        char *FILENAME_CONFIG = (char *)malloc((strlen(FILENAME_FINAL) + strlen(EXTENSION) + 1) * sizeof(char));
        FILENAME_CONFIG[0] = '\0';
        strcat(FILENAME_CONFIG, FILENAME_FINAL);
        strcat(FILENAME_CONFIG, EXTENSION);
        FILE *config_file = fopen(FILENAME_CONFIG, "w"); // Open or create the config file for writing

        if (config_file == NULL)
        {
            printf("Error opening the config file!\n");
            return 1;
        }
        fprintf(config_file, "APPROACH=%s\n", approach);
        fprintf(config_file, "K=%d\n", K);
        fprintf(config_file, "NUM_BUCKETS=%lld\n", num_buckets);
        fprintf(config_file, "BUCKET_SIZE=%lld\n", num_records_in_bucket * rounds);
        fprintf(config_file, "PREFIX_SIZE=%d\n", PREFIX_SIZE);
        fprintf(config_file, "NONCE_SIZE=%d\n", NONCE_SIZE);

        fclose(config_file); // Close the config file
        free(FILENAME_CONFIG);
    }

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

    if (writeData)
    {
        printf("Temporary File              : %s\n", FILENAME);
    }
    if (writeDataFinal)
    {
        printf("Output File Final           : %s\n", FILENAME_FINAL);
    }

    // Open the file for writing in binary mode
    FILE *fd = NULL;
    if (writeData)
    {
        fd = fopen(FILENAME, "wb+");
        if (fd == NULL)
        {
            perror("Error opening file");
            return EXIT_FAILURE;
        }
    }

    // Start walltime measurement
    double start_time = omp_get_wtime();

    // Allocate memory for the array of Buckets
    Bucket *buckets = calloc(num_buckets, sizeof(Bucket));
    if (buckets == NULL)
    {
        fprintf(stderr, "Error: Unable to allocate memory for buckets.\n");
        exit(EXIT_FAILURE);
    }

    // Allocate memory for each bucket's records
    for (unsigned long long i = 0; i < num_buckets; i++)
    {
        buckets[i].records = calloc(num_records_in_bucket, sizeof(MemoRecord));
        if (buckets[i].records == NULL)
        {
            fprintf(stderr, "Error: Unable to allocate memory for records.\n");
            exit(EXIT_FAILURE);
        }
    }

    for (unsigned long long r = 0; r < rounds; r++)
    {
        double start_time_hash = omp_get_wtime();

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
#pragma omp single
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
                                generateBlake3(record_hash, &record, &j, 0);
                                off_t bucketIndex = byteArrayToLongLong(record_hash, PREFIX_SIZE);
                                insert_record(buckets, &record, bucketIndex);
                            }
                        }
                    }
                }
            } // Implicit barrier ensures all tasks are completed before exiting
        }
        else if (strcmp(approach, "for") == 0)
        {
// Parallel For Loop Approach
#pragma omp parallel for schedule(dynamic)
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
                    generateBlake3(record_hash, &record, &j, 0);
                    off_t bucketIndex = byteArrayToLongLong(record_hash, PREFIX_SIZE);
                    insert_record(buckets, &record, bucketIndex);
                }
            }
        }

        // End hash computation time measurement
        double end_time_hash = omp_get_wtime();
        double elapsed_time_hash = end_time_hash - start_time_hash;

        // Calculate throughput (hashes per second)
        double throughput_hash = (num_hashes / elapsed_time_hash) / (1e6);
        printf("[%.2f] HashGen %.2f%%: %.2f MH/s : I/O ", omp_get_wtime() - start_time, (r + 1) * 100.0 / rounds, throughput_hash);

        // Write data to disk if required
        if (writeData)
        {
            double start_time_io = omp_get_wtime();

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
            double end_time_io = omp_get_wtime();
            double elapsed_time_io = end_time_io - start_time_io;

            // Calculate I/O throughput
            double throughput_io = (num_hashes * NONCE_SIZE) / (elapsed_time_io * 1024 * 1024);
            printf("%.2f MB/s\n", throughput_io);
        }
        else
        {
            printf("\n");
        }
    }

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

    // if (writeData)

    if (writeDataFinal && rounds > 1)
    {
        // Open the file for writing in binary mode
        FILE *fd_dest = NULL;
        if (writeDataFinal)
        {
            fd_dest = fopen(FILENAME_FINAL, "wb+");
            if (fd_dest == NULL)
            {
                perror("Error opening file");
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
        MemoRecord *buffer = malloc(buffer_size * sizeof(MemoRecord));
        if (buffer == NULL)
        {
            fprintf(stderr, "Error allocating memory for buffer.\n");
            exit(EXIT_FAILURE);
        }

        if (DEBUG)
            printf("allocating %lu bytes for bufferShuffled\n", buffer_size * sizeof(MemoRecord));
        MemoRecord *bufferShuffled = malloc(buffer_size * sizeof(MemoRecord));
        if (bufferShuffled == NULL)
        {
            fprintf(stderr, "Error allocating memory for bufferShuffled.\n");
            exit(EXIT_FAILURE);
        }

        for (unsigned long long i = 0; i < num_buckets; i = i + num_buckets_to_read)
        {
            double start_time_io2 = omp_get_wtime();

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

            if (DEBUG)
                printf("shuffling %llu buckets with %llu bytes each...\n", num_buckets_to_read * rounds, num_records_in_bucket * NONCE_SIZE);
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
            double elapsed_time_io2 = end_time_io2 - start_time_io2;
            double throughput_io2 = (num_records_in_bucket * num_buckets_to_read * rounds * NONCE_SIZE) / (elapsed_time_io2 * 1024 * 1024);
            printf("[%.2f] Shuffle %.2f%%: %.2f MB/s\n", omp_get_wtime() - start_time, (i + 1) * 100.0 / num_buckets, throughput_io2);
        }
        // end of for loop

        // Flush and close the file
        if (writeData)
        {
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

    // End total time measurement
    double end_time = omp_get_wtime();
    double elapsed_time = end_time - start_time;

    // Calculate total throughput
    double total_throughput = (num_iterations / elapsed_time) / 1e6;
    printf("Total Throughput: %.2f MH/s  %.2f MB/s\n", total_throughput, total_throughput * NONCE_SIZE);
    printf("Total Time: %.6f seconds\n", elapsed_time);

    // Call the function to count zero-value MemoRecords
    // printf("verifying efficiency of final stored file...\n");
    // count_zero_memo_records(FILENAME_FINAL);

    // Call the function to process MemoRecords
    if (VERIFY)
    {
        printf("verifying sorted order by bucketIndex of final stored file...\n");
        process_memo_records(FILENAME_FINAL, MEMORY_SIZE_bytes / sizeof(MemoRecord));
    }

    return 0;
}
