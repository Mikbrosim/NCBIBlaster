import tkinter as tk
from tkinter import messagebox,filedialog
from typing import Callable,Literal
import datetime
import os
import re

import blaster


# Modify custom_parsing as you wish :D
def custom_parsing(sequence:str, query_coverage:str|Literal["?%"], accession_number:str, match_percentage:str|Literal["?%"], total_base_pairs_in_match:int|Literal["?"], title:str):
    # The write function works like print, but writes to the output file
    write(sequence[:10], str(len(sequence)).rjust(6," "), accession_number, query_coverage, match_percentage, str(total_base_pairs_in_match).rjust(10," "), title, sep="  ")

# Number_of_alginments may also be adjusted as needed, can range between 1 and 50, hopefully
NUMBER_OF_ALIGNMENTS = 1
# MAX_HIGH_SCORING_PAIRS may also be adjusted as needed, can range between 1 and 99999, hopefully
MAX_HIGH_SCORING_PAIRS = 1


def process(file_path:str, email:str, concurrent_requests:int):
    blaster.NCBIWWW.email = email
    sequences = blaster.get_sequence(open(file_path))
    for _ in blaster.blast_batch(query_sequences=sequences,db="nr",cache_only=False,workers=concurrent_requests):
        pass


def parse(file_path:str):
    sequences = blaster.get_sequence(open(file_path))
    for seq,records in blaster.blast_batch(sequences,db="nr",cache_only=True):
        record = next(records)
        
        for acc, qc, match, bp, title in blaster.record_formatter(record,number_of_alignments=NUMBER_OF_ALIGNMENTS,max_high_scoring_pairs=MAX_HIGH_SCORING_PAIRS):
            custom_parsing(seq, qc, acc, match, bp, title)


def process_and_parse(file_path:str, email:str, concurrent_requests:int):
    process(file_path=file_path,email=email,concurrent_requests=concurrent_requests)
    parse(file_path=file_path)


file_path = None
def get_file_path():
    # Check if file_path is valid
    if not file_path or not os.path.exists(file_path):
        messagebox.showerror("Error", "Invalid file path. Please select a valid file.")
        return
    return file_path


def get_email():
    # Check if email is valid
    email = email_entry.get()
    if not email or not re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", email):
        messagebox.showerror("Error", "Invalid email format. Please enter a valid emial.")
        return
    return email


def get_concurrent_requests(_min=1,_max=10):
    # Check if concurrent_requests is a valid integer
    concurrent_requests = concurrent_requests_entry.get()
    if not concurrent_requests or not concurrent_requests.lstrip("-").isdigit():
        messagebox.showerror("Error", "Invalid concurrent requests. Please enter a valid integer.")
        return

    concurrent_requests = int(concurrent_requests)
    
    # Check if concurrent_requests is an integer in range
    if concurrent_requests < _min:
        messagebox.showerror("Warning", f"Feeling a bit too optimistic, aren't we? Let's try a number greater than {_min-1} for 'concurrent requests' and see the magic happen!")
        return
    if concurrent_requests > _max:
        messagebox.showerror("Warning", f"Whoa, slow down there! This is an intensive task for NCBI. Let's aim for a number between {_min} and {_max} for 'concurrent requests'. Manaual overwrite is required otherwise.")
        return

    return concurrent_requests


# Function to handle button click
def on_button_click(func:Callable):
    # Determine which arguments goes where


    if func==process:
        file_path = get_file_path()
        email = get_email()
        concurrent_requests = get_concurrent_requests()
        if file_path==None or email==None or concurrent_requests==None:return
        process(file_path=file_path, email=email, concurrent_requests=concurrent_requests)
        messagebox.showinfo("Done",f"Processing done, you can now parse the data or close the program")

    elif func==process_and_parse:
        file_path = get_file_path()
        email = get_email()
        concurrent_requests = get_concurrent_requests()
        if file_path==None or email==None or concurrent_requests==None:return
        process_and_parse(file_path=file_path, email=email, concurrent_requests=concurrent_requests)
        messagebox.showinfo("Done",f"Proccessing and parsing done, you can find the data in {output_file_name}")
    
    elif func==parse:
        file_path = get_file_path()
        if file_path==None:return
        parse(file_path=file_path)
        messagebox.showinfo("Done",f"Parsing done, you can find the data in {output_file_name}")
    else:
        func()


# Confirm
def confirm(msg):
    result = messagebox.askquestion("Confirmation", msg)
    if result == "yes":
        print("[+] User clicked Yes")
        return True
    else:
        print("[-] User clicked No")
        return False


def open_file_dialog():
    global file_path
    file_path = filedialog.askopenfilename()
    if file_path and os.path.exists(file_path):
        file_dialog_button.config(text = os.path.basename(file_path))

# create output file
current_time = datetime.datetime.now()
output_file_name = f"{current_time.year:02d}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}_{current_time.minute:02d}_{current_time.second:02d}.txt"
def write(*values,sep=" ",end="\n"):
    with open(output_file_name,"a") as f:
        f.write(sep.join(map(str,values))+end)

# Create the main application window
root = tk.Tk()
root.title("Blaster")
icon_path = "icon.png"
icon = tk.PhotoImage(file=icon_path)

# Set window icon
assert hasattr(root,"_w")
root.tk.call('wm', 'iconphoto', getattr(root,"_w"), icon)

# Create and pack widgets
file_path_label = tk.Label(root, text="Input File:")
file_path_label.pack()

file_dialog_button = tk.Button(root, text="Open File", command=open_file_dialog)
file_dialog_button.pack()

email_label = tk.Label(root, text="Email:")
email_label.pack()

email_entry = tk.Entry(root)
email_entry.pack()

concurrent_requests_label = tk.Label(root, text="Concurrent Requests:")
concurrent_requests_label.pack()

concurrent_requests_entry = tk.Entry(root)
concurrent_requests_entry.pack()

button_text = tk.StringVar()

process_button = tk.Button(root, text="Process", command=lambda: on_button_click(func=process))
process_button.pack()

process_and_parse_button = tk.Button(root, text="Process & Parse", command=lambda: on_button_click(func=process_and_parse))
process_and_parse_button.pack()

parse_button = tk.Button(root, text="Parse", command=lambda: on_button_click(func=parse))
parse_button.pack()

def check():
    root.after(1000, check)
check()

try:
    root.mainloop()
except KeyboardInterrupt:
    exit("\r[-] Program stopped")