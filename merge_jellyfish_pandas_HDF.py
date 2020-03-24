#!/usr/bin/env python

import os    
import sys
import pandas as pd
import argparse

# initiate the parser
parser = argparse.ArgumentParser(prog=os.path.basename(__file__))

parser.add_argument('files', nargs='*', help='files help')
parser.add_argument('-V', '--version', help='show program version', action='store_true')
parser.add_argument('-o', '--output', nargs=1, type=str, default='merged_jellyfish_HDF.csv', help='output csv filename to %(prog)s (default: %(default)s)')

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





hdf_path = 'merged_jellyfish_HDF.h5'

with pd.HDFStore(hdf_path, mode='w', complevel=5, complib='blosc') as store:
	# This compresses the final file by 5 using blosc. You can avoid that or
	# change it as per your needs.
	for filename in args.files:
		#store.append('table_name', pd.read_csv(filename, sep=','), index=False)

		basename=os.path.basename(filename)
		sample=basename.split(".")[0]	#	everything before the first "."
		print(sample)
		df = pd.read_csv(filename,
			sep=" ",
			header=None,
			usecols=[0,1],
			names=["mer",sample],
			dtype={sample: int},
			index_col=["mer"] )
		store.append('table_name', df, index=False, columns=[sample] )
		#print(store.info())
		#store.get_storer('table_name').table

	# Then create the indexes, if you need it
	#store.create_table_index('table_name', columns=['Factor1', 'Factor2'], optlevel=9, kind='full')


#	for filename in args.files:  
#		print(filename)
#		if os.path.isfile(filename) and os.path.getsize(filename) > 0:
#			basename=os.path.basename(filename)
#			sample=basename.split(".")[0]	#	everything before the first "."
#			print("Reading "+filename+": Sample "+sample)
#			d = pd.read_csv(filename,
#				sep=" ",
#				header=None,
#				usecols=[0,1],
#				names=["mer",sample],
#				dtype={sample: int},
#				index_col=["mer"] )
#			d.head()
#			d.dtypes
#			d.info(verbose=True)
#			print("Appending")
#			data_frames.append(d)
#		else:
#			print(filename + " is empty")
#	
#	if len(data_frames) > 0:
#		print("Concating all")
#		df = pd.concat(data_frames, axis=1, sort=True)
#		df.info(verbose=True)
#	
#		data_frames = []
#	
#		print("Replacing all NaN with 0")
#		df.fillna(0, inplace=True)
#		df.info(verbose=True)
#		df.head()
#		df.dtypes
#	
#		print("Converting all counts back to integers")
#		df = pd.DataFrame(df, dtype=int)
#		df.info(verbose=True)
#		df.head()
#		df.dtypes
#	
#		print("Writing CSV")
#		df.to_csv(output,index_label="mer")
#	
#	else:
#		print("No data.")


