#!/usr/bin/env python



#	https://stackoverflow.com/questions/36587211/easiest-way-to-read-csv-files-with-multiprocessing-in-pandas

import os    
import sys
import pandas as pd
import argparse
from multiprocessing import Pool

# initiate the parser
parser = argparse.ArgumentParser(prog=os.path.basename(__file__))

parser.add_argument('files', nargs='*', help='files help')
parser.add_argument('-V', '--version', help='show program version', action='store_true')
parser.add_argument('-o', '--output', nargs=1, type=str, default='merged_jellyfish_pandas_pool.csv', help='output csv filename to %(prog)s (default: %(default)s)')

# read arguments from the command line
args = parser.parse_args()

# check for --version or -V
if args.version:  
	print(os.path.basename(__file__), " 1.0")
	quit()

#	Note that nargs=1 produces a list of one item. This is different from the default, in which the item is produced by itself.
#	THAT IS JUST STUPID! And now I have to check it manually. Am I the only one?

if isinstance(args.output,list):
	output=args.output[0]
else:
	output=args.output
print( "Using output name: ", output )


data_frames = []




# wrap your csv importer in a function that can be mapped
def read_csv(filename):
	'converts a filename to a pandas dataframe'
	basename=os.path.basename(filename)
	sample=basename.split(".")[0]	#	everything before the first "."
	print("Reading "+filename+": Sample "+sample)
	return pd.read_csv(filename,
		sep=" ",
		header=None,
		usecols=[0,1],
		names=["mer",sample],
		dtype={sample: int},
		index_col=["mer"] )





## set up your pool
#with Pool(processes=16) as pool: # or whatever your hardware can support
#
#	# have your pool map the file names to dataframes
#	data_frames = pool.map(read_csv, args.files)

#with Pool(16) as pool: # or whatever your hardware can support
#	data_frames = pool.map(read_csv, args.files)

pool = Pool(16)
data_frames = pool.map(read_csv, args.files)



	# reduce the list of dataframes to a single dataframe
	#combined_df = pd.concat(data_frames, ignore_index=True)
#	combined_df = pd.concat(data_frames, axis=1, sort=True)


if len(data_frames) > 0:
	print("Concating all")
	df = pd.concat(data_frames, axis=1, sort=True)
	df.info(verbose=True)

	print(df.head())

	data_frames = []

	print("Replacing all NaN with 0")
	df.fillna(0, inplace=True)
	df.info(verbose=True)
	print(df.head())
	df.dtypes

	print("Converting all counts back to integers")
	df = pd.DataFrame(df, dtype=int)
	df.info(verbose=True)
	print(df.head())
	df.dtypes

	print("Writing CSV")
	df.to_csv(output,index_label="mer")

else:
	print("No data.")


