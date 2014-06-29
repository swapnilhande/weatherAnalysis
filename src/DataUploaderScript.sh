#!/bin/bash
# shell script to upload dataset to S3
# Initial cleanup
printf "Initial cleanup\n"
cd /home/swapnil/Downloads/UntarDataset/; 
rm -rf *;
cd /home/swapnil/Downloads/UnzippedDataset/; 
rm -rf *; 

# Go to the dataset directory having tar files
# For all the years, untar, unzip and upload
for (( i = 1929; i <= 2014; i++ ))
do
	printf "Year: $i\n";
	cd /home/swapnil/Downloads/Dataset/;
	# ---------------------------------------------------------------
	# For every file untar it to the untar files location
	# ---------------------------------------------------------------
	for a in `ls -1 gsod_$i.tar`
	do 
		tar -xf $a -C /home/swapnil/Downloads/UntarDataset/
	done
	printf "Untar done.\n"
	# ---------------------------------------------------------------
	# Go to the untar files location
	# ---------------------------------------------------------------
	cd /home/swapnil/Downloads/UntarDataset/;
	# ---------------------------------------------------------------
	# For every zip file, unzip it to unzip location
	# ---------------------------------------------------------------
	for a in `ls -1 *.gz`
	do 
		gunzip -c $a>/home/swapnil/Downloads/UnzippedDataset/$a.txt
	done
	printf "Unzip done.\n"
	# ---------------------------------------------------------------
	# Go to untar location and delete all files
	# ---------------------------------------------------------------
	cd /home/swapnil/Downloads/UntarDataset/; 
	rm -rf *;
	printf "Untar files removed.\n"
	# ---------------------------------------------------------------
	# Go to unzipped location and upload all files to S3
	# ---------------------------------------------------------------
	cd /home/swapnil/Downloads/UnzippedDataset/;
	s3cmd put *.txt s3://cs6240data/
	printf "Uploaded to S3.\n"
	# ---------------------------------------------------------------
	# Go to unzip location and delete all files
	# ---------------------------------------------------------------
	cd /home/swapnil/Downloads/UnzippedDataset/; 
	#rm -rf *; 
	printf "Unzip files removed.\n"
done
