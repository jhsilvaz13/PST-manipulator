import os
import sys
import argparse
import logging

import pypff
import unicodecsv as csv
import chardet
__author__ = 'Jhon Jairo Silva Zabala'
__date__ = '20231227'
__version__ = 0.01
__description__ = 'SOME DESCRIPTION'


output_directory = "./data/"

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def main(pst_file):
    """
    The main function opens a PST and calls functions to parse and report data from the PST
    :param pst_file: A string representing the path to the PST file to analyze
    :param report_name: Name of the report title (if supplied by the user)
    :return: None
    """
    # Detect the encoding of the PST file
    encoding = detect_encoding(pst_file)

    # Check if the detected encoding is UTF-8
    if encoding.lower() != 'utf-8':
        raise ValueError(f"The PST file is not encoded in UTF-8. Detected encoding: {encoding}")
    logging.debug("Opening PST for processing...")
    opst = pypff.open(pst_file)
    root = opst.get_root_folder()

    logging.debug("Starting traverse of PST structure...")
    folderTraverse(root)

    

def makePath(file_name):
    """
    The makePath function provides an absolute path between the output_directory and a file
    :param file_name: A string representing a file name
    :return: A string representing the path to a specified file
    """
    return os.path.abspath(os.path.join(output_directory, file_name))


def folderTraverse(base):
    """
    The folderTraverse function walks through the base of the folder and scans for sub-folders and messages
    :param base: Base folder to scan for new items within the folder.
    :return: None
    """
    for folder in base.sub_folders:
        if folder.number_of_sub_folders:
            folderTraverse(folder) # Call new folder to traverse:
        checkForMessages(folder)


def checkForMessages(folder):
    """
    The checkForMessages function reads folder messages if present and passes them to the report function
    :param folder: pypff.Folder object
    :return: Non
    """
    logging.debug("Processing Folder: " + folder.name)
    message_list = []
    for message in folder.sub_messages:
        message_dict = processMessage(message)
        message_list.append(message_dict)
    folderReport(message_list, folder.name)


def processMessage(message: pypff.message):
    """
    The processMessage function processes multi-field messages to simplify collection of information
    :param message: pypff.Message object
    :return: A dictionary with message fields (values) and their data (keys)
    """
    return {
        "subject": message.subject,
        "sender": message.sender_name,
        "body": message.plain_text_body,
        "creation_time": message.creation_time,
        "submit_time": message.client_submit_time,
        "delivery_time": message.delivery_time,
    }


def folderReport(message_list, folder_name):
    """
    The folderReport function generates a report per PST folder
    :param message_list: A list of messages discovered during scans
    :folder_name: The name of an Outlook folder within a PST
    :return: None
    """
    if not len(message_list):
        logging.warning("Empty message not processed")
        return

    # CSV Report
    fout_path = makePath("folder_report_" + folder_name + ".csv")
    fout = open(fout_path, 'wb')
    header = ['creation_time', 'submit_time', 'delivery_time',
              'sender', 'subject', 'body']
    csv_fout = csv.DictWriter(fout, fieldnames=header, extrasaction='ignore')
    csv_fout.writeheader()
    csv_fout.writerows(message_list)
    fout.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser( description=__description__,
                                     epilog='Developed by ' + __author__ + ' on ' + __date__)
    parser.add_argument('PST_FILE', help="PST File Format from Microsoft Outlook")
    parser.add_argument('OUTPUT_DIR', help="Directory of output for temporary and report files.")
    parser.add_argument('-l', help='File path of log file.')
    args = parser.parse_args()

    output_directory = os.path.abspath(args.OUTPUT_DIR)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    if args.l:
        if not os.path.exists(args.l):
            os.makedirs(args.l)
        log_path = os.path.join(args.l, 'pst_indexer.log')
    else:
        log_path = 'pst_indexer.log'
    logging.basicConfig(filename=log_path, level=logging.DEBUG,
                        format='%(asctime)s | %(levelname)s | %(message)s', filemode='a')

    logging.info('Starting PST_Indexer v.' + str(__version__))
    logging.debug('System ' + sys.platform)
    logging.debug('Version ' + sys.version)

    logging.info('Starting Script...')
    main(args.PST_FILE)
    logging.info('Script Complete')