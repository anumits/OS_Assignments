#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>


int main(int argc, char *argv[])
{
	DIR *dir;	//directory stream
	FILE *file;	//file stream
	struct dirent *ent;	// directory entry structure
	char *line = NULL;	// pointer to 
	size_t len = 1000;	//the length of bytes getline will allocate
	size_t read;

	char full_filename[256];	//will hold the entire file name to read		
	

	// check the arguments
	if(argc < 2)
	{
		printf("Not enough arguments supplied\n");
		return -1;
	}

	if(argc > 2)
	{
		printf("Too many arguments supplied\n");
		return -1;
	}


	// try to open the directory given by the argument
	if ((dir = opendir (argv[1])) != NULL) 
	{
	  	/* print all the files and directories within directory */
	  	while ((ent = readdir (dir)) != NULL) 
		{
	    		printf ("%s\n", ent->d_name);
			// Check if the list is a regular file
			if(ent->d_type == DT_REG)
			{
				// Create the absolute path of the filename
				snprintf(full_filename, sizeof full_filename, "./%s%s\0", argv[1], ent->d_name);				
				// open the file
				file = fopen(full_filename, "r");
				// file was not able to be open
				if (file != NULL)
				{
					// Print out each line in the file
					while ((read = getline(&line, &len, file)) != -1) 				{
    						printf("Retrieved line of length %d:\n", read);
    						printf("%s", line);
					}
					fclose(file);
				}			
			}
	  	}
		// Close the directory structure
	  	closedir (dir);
	} 
	else 
	{
	  	/* could not open directory */
	  	perror ("");
  		return -1;
	}
return 0;
}
