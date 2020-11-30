#!/bin/sh

# Simple script to produce a self-extracting compressed binary

export compressed_binary=eliasdb_compressed

echo "cat \$0 | sed '1,/#### Binary ####/d' | gzip -d > ./__e" > $compressed_binary
echo "chmod ugo+x ./__e" >> $compressed_binary
echo "mv ./__e ./\$0" >> $compressed_binary
echo "./\$0" >> $compressed_binary
echo "exit 0" >> $compressed_binary
echo "This is a simple shell script trying to unpack the binary data" >> $compressed_binary
echo "after the marker below. Unpack manually by deleting all lines" >> $compressed_binary
echo "up to and including the marker line and do a gzip -d on the" >> $compressed_binary
echo "binary data" >> $compressed_binary
echo "#### Binary ####" >> $compressed_binary
gzip -c eliasdb >> $compressed_binary
chmod ugo+x $compressed_binary
