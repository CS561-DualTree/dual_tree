#!/bin/bash

ANALYSIS_PROGRAM="analysis.o"
if [ ! -f "$ANALYSIS_PROGRAM" ]
then
    echo "Use \"make\" command before run the script, because the file ${ANALYSIS_PROGRAM} is needed"
    exit 1
fi

if [ $# -ne 2 ]
then 
    echo "Two arguments are needed: the first one should be a directory in which data files are and
        the second one should be a directory in which output of the analysis will be"
    exit 1
fi

if [ ! -d $2 ]; then
    echo "Creating output folder $2"
    mkdir $2
fi

analysis_folder() {
    folder_name=$1
    for f in "$folder_name"/*; do
        echo $f
        if [ -d $f ]; then
            analysis_folder $f $2
        else
            output_file_name="$2/${folder_name////-}-res"
            ./analysis.o $f >> $output_file_name
            sleep 3s
        fi
    done
}

analysis_folder $1 $2
