import concurrent.futures
import hashlib
import time
import os
import io

try:
    #from Bio import SeqIO
    from Bio.Blast import NCBIWWW,NCBIXML
    from Bio.Blast.Record import Blast,Alignment,HSP
    from Bio.Seq import Seq
except ImportError:
    print("[!] Could not import biopython")
    print("[.] To install it, run the following command 'pip install biopython'")
    print("[.] If pip is not installed already, make sure to run 'python -m ensurepip --upgrade' first")
    input("[?] Press enter to close")
    exit()

CACHE_FOLDER = "cache"
ALLOWED_BASES = set("ATCGU")


def main():
    data_file = open("test.txt")
    dnas:list[Seq] = []
    for i,dna in enumerate(get_sequence(file=data_file)):
        print(i,dna[:10])
        dnas.append(dna)

    for seq,records in blast_batch(dnas,db="nr"):
        record = next(records)
        print(f"== {seq[:10]} ==")
        for acc, qc, match, bp, title in record_formatter(record,8,1):
            #pass
            print(f'{acc} {qc} {match} {bp} {title}')


def record_formatter(record:Blast,number_of_alignments:int=2,max_high_scoring_pairs:int=1):
    # TODO This could use some cleaning
    if not isinstance(record,Blast):
        raise TypeError(f"hsp had unknown type, {type(record)}, expected Bio.Blast.Record")
    alignments = list(record.alignments)
    for alignment in alignments[:number_of_alignments]:
        # Make sure data is as it should be
        if not isinstance(alignment,Alignment):
            raise TypeError(f"alignment had unknown type, {type(alignment)}, expected Bio.Blast.Record.Alignment")
        acc = alignment.hit_id.split("|")[-2].strip()
        title = alignment.hit_def.strip()
        hsps = list(alignment.hsps)

        # Calculate query coverage
        hsp_pairs = []
        for hsp in hsps:
            if not isinstance(hsp,HSP):
                raise TypeError(f"hsp had unknown type, {type(hsp)}, expected Bio.Blast.Record.HSP")
            if isinstance(hsp.query_start,int) and isinstance(hsp.query_start,int) and isinstance(record.query_length,int):
                hsp_pairs.append([hsp.query_start,hsp.query_end])
        hsp_pairs.sort(key=lambda x:x[0])
        stack = hsp_pairs[:1]
        for i in hsp_pairs[1:]:
            if stack[-1][0] <= i[0] <= stack[-1][-1]:
                stack[-1][-1] = max(stack[-1][-1], i[-1])
            else:
                stack.append(i)
        hsp_pairs = stack
        qc = 0
        for interval in hsp_pairs:
            qc += interval[1]-interval[0]+1

        # Extract
        for hsp in hsps[:max_high_scoring_pairs]:
            if not isinstance(hsp,HSP):
                raise TypeError(f"hsp had unknown type, {type(hsp)}, expected Bio.Blast.Record.HSP")
            if isinstance(hsp.identities,int) and isinstance(hsp.align_length,int):
                match = f"{(hsp.identities/hsp.align_length)*100:0.2f}%".rjust(7," ")
            else:
                match = f"?%"
            if isinstance(alignment.length,int):
                bp = alignment.length
            else:
                bp = "?"
            if qc != 0 and isinstance(record.query_length,int):
                qc = f"{(qc/record.query_length)*100:0.2f}%".rjust(7," ")
            else:
                qc = "?"
            yield acc, qc, match, bp, title


def blast_batch(query_sequences:list[Seq], db="nr", cache_only=True, workers:int=1):
    remove_empty_cache()
    print("[.] Running blast!")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(lambda query_sequence: blast(query_sequence,db,cache_only),query_sequence): query_sequence for query_sequence in query_sequences}
        for future in concurrent.futures.as_completed(futures):
            yield future.result()
    print("[.] Blast done!")


def blast(query_sequence:Seq|str,db="nr",cache_only=False):
    # Arg check
    if not isinstance(query_sequence,(Seq,str)):
        raise TypeError(f"Got query_sequence of type {type(query_sequence)}, expected Seq or str")
    query_sequence = str(query_sequence)

    # Calculate MD5_sum for cache
    md5_checksum = hashlib.md5(query_sequence.encode()).hexdigest()
    file_name = os.path.join(CACHE_FOLDER,f"{md5_checksum}.xml")

    # Cache exists great, if not run blast
    if os.path.exists(file_name):
        print(f"[.] {query_sequence[:10]} {md5_checksum} Cache found")
    else:
        # Only run blast if allowed to do so
        if cache_only:
            print(f"[!] Only cache is allowed, but sequence {query_sequence[:10]} {md5_checksum}, is not in cache")
            open(file_name, "w").close()
        else:
            # Run blast and save result to cache
            print(f"[.] {query_sequence[:10]} {md5_checksum} Locking cache_file")
            with open(file_name, "w") as file:
                print(f"[.] {query_sequence[:10]} Running blast with {len(query_sequence)} BP")
                t = time.time()
                result_handle:io.StringIO = NCBIWWW.qblast(program="blastn",database=db,sequence=query_sequence,megablast=True)
                if not isinstance(result_handle,io.StringIO):
                    raise TypeError(f"result_handle returned type {type(result_handle)} expected io.StringIO")
                result =  result_handle.getvalue()
                print(f"[.] {query_sequence[:10]} Blast took {int(time.time()-t)} seconds")
                print(f"[.] {query_sequence[:10]} Saving to cache")
                file.write(result)

    if len(open(file_name).read(1))==0:
        print(f"[!] Cache file at '{file_name}' is empty")
    blast_records = NCBIXML.parse(open(file_name))
    return (query_sequence, blast_records)


def remove_empty_cache():
    print("[.] Removing empty cache files")
    # Removes empty files from cache folder
    for filename in os.listdir(CACHE_FOLDER):
        if filename.endswith(".xml") and len(filename)==36:
            file_path = os.path.join(CACHE_FOLDER, filename)
            # Check if the file is empty
            if os.path.getsize(file_path) == 0:
                # Remove the empty XML file
                try:
                    os.remove(file_path)
                    print(f"[.] Removed empty cache file: {file_path}")
                except Exception as e:
                    print(f"[.] Couldn't empty cache file: {file_path} due to the following error `{e}` if this error persists, try removing the file manually")


def parser(file:io.TextIOWrapper):
    # TODO Implement file type checks
    # TODO Implement support for other file types
    print("[.] Parsing sequences")
    if not isinstance(file,io.TextIOWrapper):
        raise TypeError(f"Got file argument, which is not a type, got {type(file)}")
    data = file.read().strip()
    # TODO Make this more stable
    if data.startswith(">"):
        print("[.] Reading fasta file")
        sections = data.split(">")
        for section in sections[1:]:
            metadata, sequence = section.split("\n",1)
            yield {
                "metadata":metadata,
                "sequence":sequence.replace("\n",""),
            }
        # This is a fasta file
    else:
        print("[.] Reading fastq file")
        # This is a fastq file ???
        lines = data.splitlines()
        if len(lines)%4!=0:
            raise ValueError(f"Line count of '{file.name}' must be divideable by 4, line count {len(lines)}")
        for i in range(len(lines)//4):
            sequence = lines[4*i+1]
            if not set(sequence).issubset(ALLOWED_BASES):
                raise ValueError(f"Got a sequence containing {set(sequence)}, which is not a subset of {ALLOWED_BASES}")
            yield {
                "metadata":lines[4*i],
                "sequence":sequence,
                "plus":lines[4*i+2],
                "some_other_data":lines[4*i+3]
            }


def get_sequence(file:io.TextIOWrapper):
    sequences:list[Seq] = []
    for i,data in enumerate(parser(file=file)):
        if "sequence" in data:
            sequence = data["sequence"]
            sequences.append(Seq(sequence))
            print(i,sequence[:10])
    return sequences


if __name__=="__main__":
    main()