# table_merging

The problem at hand is how to merge many, very large csv files.


More specifically, how can I merge the mer counts output by jellyfish.


```
jellyfish count -C -m 13 -s 1G -o hairpin.jf hairpin.fa 
jellyfish count -C -m 13 -s 1G -o mature.jf mature.fa 
jellyfish dump -column -o mature.csv mature.jf 
jellyfish dump -column -o hairpin.csv hairpin.jf 
```

This yields a space-delimited file containing ...

```
head mature.csv 
AAAAAAAAAAGCC 4
AAAAAAAAAAGGA 1
AAAAAAAAAGCCC 4
AAAAAAAAAGGCC 1
AAAAAAAAGCCCA 4
AAAAAAAAGGCCA 1
AAAAAAAGAAAGA 1
AAAAAAAGCCCAA 4
AAAAAAAGGCCAA 1
AAAAAACAAAGGG 1
```


I have successfully merged these with python and pandas.
Read each file into a dataframe and append it to an array.
Then concat all of these as seen in the simple example.

```
pip install --upgrade --user pandas

./merge_jellyfish_pandas.py mature.csv hairpin.csv 
```

With 77 files, each of which takes about 250MB, I quickly run low on memory, but it does complete hours later.

If I try again with k=15, rather than 13, I quickly run out of memory and it fails.




I have also tried using a database like sqlite3

https://www.quora.com/How-do-I-merge-8-CSV-files-49-million-rows-each-with-a-common-column-and-export-the-final-output-into-a-CSV-in-a-Core-i7-8GB-RAM-PC

Simple load each csv file into its own table, then select and join them on the mer column.

```
cat sqlitemerge.create.sql | sqlite3 sqlitemerge.db
cat sqlitemerge.select.sql | sqlite3 sqlitemerge.db
head results.csv
mer,hairpin,mature
AAAAAAAAAAGCC,6,4
AAAAAAAAAAGGA,2,1
AAAAAAAAAGCCC,6,4
AAAAAAAAAGGCC,2,1
AAAAAAAAGCCCA,6,4
AAAAAAAAGGCCA,1,1
AAAAAAAGAAAGA,3,1
AAAAAAAGCCCAA,6,4
AAAAAAAGGCCAA,1,1
```

Sadly, sqlite3 does not support a full outer join, only a left outer join.
This means if the mer isn't in the first table, it won't be included.

Perhaps this will work with MySQL, but for the moment I don't have access 
to MySQL on the cluster.

















I can also do this with dask, which some have suggested is much better.
The following example has be tried to ensure works, but I haven't run on the large sample yet.

```
pip install --upgrade --user dask toolz fsspec

./merge_jellyfish_dask.py mature.csv hairpin.csv 
```













In addition, I could also try python, pandas and HDF

https://stackoverflow.com/questions/38799704/efficient-merge-for-many-huge-csv-files

import os
import glob
import pandas as pd
os.chdir("\\path\\containing\\files")

files = glob.glob("*.csv")
hdf_path = 'my_concatenated_file.h5'

with pd.HDFStore(hdf_path, mode='w', complevel=5, complib='blosc') as store:
    # This compresses the final file by 5 using blosc. You can avoid that or
    # change it as per your needs.
    for filename in files:
        store.append('table_name', pd.read_csv(filename, sep=','), index=False)
    # Then create the indexes, if you need it
    store.create_table_index('table_name', columns=['Factor1', 'Factor2'], optlevel=9, kind='full')



















