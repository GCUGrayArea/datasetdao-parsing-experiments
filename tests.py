import os
import hashlib
from fastwarc.stream_io import *
from fastwarc.warc import ArchiveIterator
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.encoding import bytes_to_str, EncodingDetector
from datetime import datetime
import trafilatura

def run_resiliparse(warc, home_dir = None):
    start_time = datetime.now()
    det = EncodingDetector()
    count_records = 0
    HOME = os.path.expanduser("~")+os.sep
    OUTPUT_FOLDER = "resiliparse-output" + os.sep

    if not os.path.exists(HOME + OUTPUT_FOLDER):
        os.makedirs(HOME + OUTPUT_FOLDER)

    for record in ArchiveIterator(GZipStream(FileStream((HOME if home_dir is None else home_dir)+warc,'rb')), func_filter=lambda r: r.headers.get('WARC-Identified-Payload-Type') == 'text/html'):
        det.reset()
        text = record.reader.read() # will be bytes due to read mode of the file object
        sha256_hash = hashlib.sha256(text).hexdigest()
        det.update(text)
        text_string = bytes_to_str(text, det.encoding())
        outfile = sha256_hash + ".txt"
        with open(HOME + OUTPUT_FOLDER + outfile, ('w' if os.path.exists(HOME + OUTPUT_FOLDER + outfile) else 'x')) as out:
            out.writelines(extract_plain_text(text_string))
        count_records += 1

    stop_time = datetime.now()
    elapsed_time = (stop_time - start_time).total_seconds()
    return count_records, elapsed_time


def run_trafilatura(warc, home_dir = None):
    start_time = datetime.now()
    det = EncodingDetector()
    count_records = 0
    count_decode_errors = 0
    HOME = os.path.expanduser("~")+os.sep
    OUTPUT_FOLDER = "trafilatura-output" + os.sep

    if not os.path.exists(HOME + OUTPUT_FOLDER):
        os.makedirs(HOME + OUTPUT_FOLDER)

    for record in ArchiveIterator(GZipStream(FileStream((HOME if home_dir is None else home_dir)+warc,'rb')), func_filter=lambda r: r.headers.get('WARC-Identified-Payload-Type') == 'text/html'):
        det.reset()
        text = record.reader.read() # will be bytes due to read mode of the file object
        sha256_hash = hashlib.sha256(text).hexdigest()
        det.update(text)
        try:
            text_string = bytes_to_str(text, det.encoding())
            outfile = sha256_hash + ".txt"
            with open(HOME + OUTPUT_FOLDER + outfile, ('w' if os.path.exists(HOME + OUTPUT_FOLDER + outfile) else 'x')) as out:
                out.writelines(trafilatura.extract(text_string))
        except Exception:
            pass
    stop_time = datetime.now()
    elapsed_time = (stop_time - start_time).total_seconds()
    print(str(count_decode_errors) + " decoding errors")
    return count_records, elapsed_time

if __name__ == "__main__":
    out = run_resiliparse('CC-MAIN-20231128083443-20231128113443-00000.warc.gz') # default
    print("Starting resiliparse test")
    print("Records extracted: " + str(out[0]))
    print("Elapsed time: " + str(out[1]))
    out = run_trafilatura('CC-MAIN-20231128083443-20231128113443-00000.warc.gz')
    print("Starting trafilatura test")
    print("Records extracted: " + str(out[0]))
    print("Elapsed time: " + str(out[1]))
