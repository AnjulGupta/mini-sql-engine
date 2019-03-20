if [[ "$#" -ne 1 ]]; then
	echo "Error :Incorrect format,   ./<scriptname> '<query>'"
	exit 1
fi
python 2018201021.py "$1"
