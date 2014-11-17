#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <pthread.h>
#include "strmap.h"

/**
 * multiFileParse
 *
 * Implementation: Using Map datastructure to record the ip address we have found in all the threads.
 * If the ip address already in the map, we ignore it, otherwise we add the ip into the map.
 * Everytime we add an ip into the map, we increase the global variable ip_count by 1. 
 * Finally, we output ip_count as the result. During the whole procedure, our global ip_count and map
 * are well protected by mutex variable map_lock, so that only one thread can update and access the shared data.
 * 
 */

int num_files;  // The number of files in the directory we are going through
int num_threads;  // Number of threads being used
int num_distinct_IPs;  // Global Variable to count the number of distinct IPs
char *dir_name; // Name of directory

pthread_mutex_t map_lock; 
StrMap *map;

void *readFiles(void *thread_id); 
int countFiles(DIR *dir);

int main(int argc, char *argv[]) {
	
	// There is an extra argument here. This is to specify how many threads are to be created
	// In the next few lines, we check if the number of arguments and their types are correct: 
	static const char *ERROR_ARGUMENTS = "Expected arguments <directory> <number of threads>";
	static const int MAP_SIZE = 200; // using a size of 200 for the map, refer below to see what the map is and why it is used

	DIR *dir;	// directory stream

	// check the number of arguments
	if(argc < 3) {
		printf("Not enough arguments supplied. \n%s \n", ERROR_ARGUMENTS);
		return -1;
	}
	if(argc > 3) {
		printf("Too many arguments supplied. \n%s \n", ERROR_ARGUMENTS);
		return -1;
	}

	// Check if the directory exists and is valid
	dir_name = argv[1];
	if ((dir = opendir(dir_name)) == NULL) {
		printf("Error: cannot open directory (%s) \n", argv[1]);
		return -1;
	}

	// Check the argument of the number of threads
	if ((num_threads = atoi(argv[2])) <= 0) {
		printf("Error: Invalid arguemnts. The number of threads should > 0.");
		return -1;
	}

	printf("Directory with files to be parsed is: %s\n", dir_name);
	printf("Number of threads for the multithreaded mode: %d\n", num_threads);


	/* We now create a map data structure. 
	 * This is basically a hash function with a key value pair with 
	 * the key always being unique. This helps us check if any new
	 * IP addresses we come across already exists in a much more efficient
	 * manner than if we used simpler data structures like linked lists
	 * or two dimensional arrays which have other space problems too.
	 * 
	 * The data structure is used here with the help of Strmap.h and
	 * strmap.c, which are both available for public use through the
	 * GNU Lesser General Public License. (http://pokristensson.com/strmap.html)
	 */

	map = sm_new(MAP_SIZE); // Here the strmap data structure is created
	if (map == NULL) {
		printf("Error: Create map failed."); // To check for errors
		return -1;
	}
	
	// Initializing the mutex objects
  	pthread_mutex_init(&map_lock, NULL);

  	/* Count the number of the files under the directory */
	printf("\nGoing through the files:\n");
	num_files = 0;
	struct dirent *ent;	// directory entry structure
	while ((ent = readdir (dir)) != NULL)  {
		if(ent->d_type == DT_REG) {
			num_files++;
		}
	}
	
	closedir (dir); // Close the directory structure
	printf("Number of files that have to be read is %d\n", num_files);

	pthread_t threads[num_threads];
	pthread_attr_t attr;

	// Get the default attributes
	pthread_attr_init(&attr);
	int result;

	// Create the threads:
	long i; // Long because pthreads_create requires it
	for (i = 0; i < num_threads; i++) {
		printf("Creating thread number %ld\n", i+1);
		result = pthread_create(&threads[i], &attr, readFiles, (void *)i);  // creating the pthreads and the readFiles method is passed here
		if (result) {
			printf("Error: return error code from pthread_create() [%d]\n", result);
			exit(-1);
		}
	}

	// Wait for all threads to finish:
	for (i = 0; i < num_threads; i++) {
		pthread_join(threads[i], NULL);
	}

	printf("\n\nHence the total number of distinct IP addresses is %d\n", num_distinct_IPs);


	// Clean up the threads
	pthread_attr_destroy(&attr);
	pthread_mutex_destroy(&map_lock);
	pthread_exit(NULL);

	return 0;
}


/* This is the funciton that goes through the file, where the mutex lock happens, 
 * during which the check happens within the map and the global variable is incremented if needed
 */
void *readFiles(void *thread_id) { 
	int num = num_files / num_threads; // how many files need to process
	int tid = (long)thread_id; // conversion to long
	int partition = tid * num; // calculate the partitions
	printf("This is the readFiles of thread number %d\n",tid+1);
	printf("Number of files this thread processes: %d\n", num);
	printf ("Hence the first file in this partition will be %d\n", partition);

	DIR *dir;	//directory stream
	FILE *file;	//file stream
	char full_filename[256];	//will hold the entire file name to read

	if ((dir = opendir (dir_name)) != NULL)  {
		int i = partition + 1;
		while (1) {
			int last_one = tid == num_threads - 1;
			if (last_one &&  i > num_files) { // To break out of while loop if it's the last one
				break;
			}
			else if (!last_one && i > partition + num) {
				break;
			}
			// open the file
			snprintf(full_filename, sizeof full_filename, "./%saccess%d.log", dir_name, i);
			file = fopen(full_filename, "r");
			if (file == NULL) {
				printf("Error: Not found file %s\n", full_filename); // Checking if file exists
			}
			else {
				long lines = 0;
				char *line = NULL;	// pointer to 
				size_t len = 1000;	// the length of bytes getline will allocate
				size_t read;
				char *ip = NULL;
				
				while ((read = getline(&line, &len, file)) != -1) {
					lines++;
					ip = strtok (line, " "); // Splitting into tokens with ' ' as the delimiter
					pthread_mutex_lock(&map_lock);  // lock the mutex associated with minimum_value and update the variable as required
					int flag = sm_exists(map, ip); // To check if the IP address is something that has already been seen, that is, not distinct
					if (flag == 0) { // Isn't present already, so add to the map
						sm_put(map, ip, ""); // The actual adding to the map
						num_distinct_IPs++; // Increment the count of distinct IP addresses
					}
					pthread_mutex_unlock(&map_lock); // unlock the mutex
				}
				printf("File %d is being accessed now, and this is under thread %d.", i, tid);
				printf(" Its file name is %s and the number of lines within the file is %ld.\n", full_filename, lines);
				fclose(file);
			}
			i++; // Moving on to the next file
		}
	}

	closedir(dir);

	pthread_exit(NULL); // terminate the thread
}


