# Usage
1. Unzip the [zip file](https://github.com/Mikbrosim/NCBIBlaster/archive/refs/heads/main.zip) and then run the gui.py program to run it with graphics. You may need to install biopython if the python package is not already installed, if not, it should tell you which commands to run.
2. Select a fasta or fastq file
3. Enter email (an email that can be contacted if you are using their api/database too much and they would like you to slow down a bit, probably never happens)
4. Enter how many BLAST queries it should make to the database at a time, currently set to accept from 1 to 10, the higher the number, the faster it goes
5. Then press "Process" to blast, this should take ~20 seconds per sequence depending on their length. Could be in the ball park of 2000 seconds, if the sequence is long. 
6. Press "Parse", to turn the data from the database into a reasonably readable text file, this format can be changed at the top of the gui.py file in its "custom_parsing" function, if you have a little python know how