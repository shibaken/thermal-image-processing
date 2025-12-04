#! /bin/bash
folder_path=`pwd`
echo $folder_path
full_filename=$1
uploads_folder_path=$2

mkdir -p "$folder_path/logs"
LOG_FILE="$folder_path/logs/log.txt"

if [ -z "$full_filename" ]; then
    echo "No filename provided"
    printf "\nNo filename provided\n" >> "$LOG_FILE"
    exit 1
fi

if [ -z "$uploads_folder_path" ]; then
    echo "No uploads folder path provided"
    printf "\nNo uploads folder path provided\n" >> "$LOG_FILE"
    exit 1
fi
filename="${full_filename%.7z*}.7z"
basename_filename=`basename $filename`
timestamp=$(date +"%Y-%m-%d_%H:%M:%S")
printf "\n%s filename: %s" "$timestamp" "$filename" >> "$LOG_FILE"
#if [[ $filename == *.7z ]]; then

#dirname="${basename_filename%.*}"
# First, remove the file extension (e.g., .7z)
temp_dirname="${basename_filename%.*}"
# Next, remove the timestamp suffix added during the upload process (e.g., .20251125_110724)
dirname="${temp_dirname%.*}"

printf "\ndirname: $dirname" >> "$LOG_FILE"
sleep 10
echo "STARTING"
echo $filename

# Create directory to prevent error if it does not exist
mkdir -p $folder_path/thermal_data_processing/

cp $filename   $folder_path/thermal_data_processing/$basename_filename
mv $filename   $uploads_folder_path/$basename_filename
cd $folder_path/thermal_data_processing/
echo "Uncompressing file $filename"

7z x $basename_filename -aoa
rm $basename_filename

echo $dirname
echo "Run pythonscript"

python -u $folder_path/thermalimageprocessing/thermal_image_processing.py  $folder_path/thermal_data_processing/$dirname

end_timestamp=$(date +"%Y-%m-%d_%H:%M:%S")
printf "\n%s %s done" "$end_timestamp" "$dirname"  >> "$LOG_FILE"
