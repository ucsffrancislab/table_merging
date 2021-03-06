#!/usr/bin/env python

import os    
import sys
import dask.dataframe as dd
import pandas as pd
import argparse

# initiate the parser
parser = argparse.ArgumentParser(prog=os.path.basename(__file__))

parser.add_argument('files', nargs='*', help='files help')
parser.add_argument('-V', '--version', help='show program version', action='store_true')
parser.add_argument('-o', '--output', nargs=1, type=str, default='merged_jellyfish_dask.csv', help='output csv filename to %(prog)s (default: %(default)s)')

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

for filename in args.files:  
	print(filename)
	if os.path.isfile(filename) and os.path.getsize(filename) > 0:
		basename=os.path.basename(filename)
		sample=basename.split(".")[0]	#	everything before the first "."
		print("Reading "+filename+": Sample "+sample)
		d = dd.read_csv(filename,
			sep=" ",
			header=None,
			usecols=[0,1],
			names=["mer",sample],
			dtype={sample: int}).set_index('mer')
		d.head()
		d.dtypes
		d.info(verbose=True)
		print("Appending")
		data_frames.append(d)
	else:
		print(filename + " is empty")


if len(data_frames) > 0:
	print("Concating all")
	df = dd.concat(data_frames, axis=1)	#, sort=True)
	#df.reset_index()
	#df.set_index('mer')
	df.info(verbose=True)
	print(df.head())

	data_frames = []

	print("Replacing all NaN with 0")
	#df.fillna(0, inplace=True)
	df = df.fillna(0)
	df.info(verbose=True)
	print(df.head())
	df.dtypes

	print("Converting all counts back to integers")
	df = df.astype(int)
	#df = pd.DataFrame(df, dtype=int)
	####df = df.compute()
	df.info(verbose=True)
	print(df.head())
	df.dtypes

	print("Writing CSV")
	df.to_csv(output,index_label="mer",single_file=True)

else:
	print("No data.")


