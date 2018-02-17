# Written by: Daniel Bowder
# Started on: 2018-01-15 -- 2018 Jan 15
# Github: https://github.com/DotBowder
#
# Version: 0.3
#
# This program takes in the XML, enwiki wikimedia data dump for Wikipedia pages.
#
# This program was designed to take the 64GB xml file, enwiki-20170820-pages-articles.xml,
# find hyperlinks on each page, and determine what wikipedia pages link to other wikipedia pages.
#
# The end goal of this program is to use neo4j to visualize, or just store, information
# pertaining to the page to page hyperlinking relationships of wikipedia.
#
# This can serve as a tool for discovering how different topics realate to eachother,
# as characterized by wikipedia linking relationships.
#
# This program uses lxml to parse the xml file, and extract useful information such as
# website links, titles, and other information located on a wikipedia page.
#
# I'm happy to say the wasteful full pass of the data file is no longer necessary for generating
# the original lookup table. The code now completes one full pass of the data file, and extracts
# the Wikipedia ID, Wikipedia Page Title, and all WikiText Hyperlinks on the webpage.


# Helpful links:
#
# Wikipedia "wikitext" parsing
# https://en.wikipedia.org/wiki/Help:Wikitext#Links_and_URLs
#
# Wikipedia Data Dump Torrents
# https://meta.wikimedia.org/wiki/Data_dump_torrents
#
# lxml syntax
# http://lxml.de/parsing.html

import time, sys, random, getpass
from lxml import etree as ET
from py2neo import authenticate, Graph, Node, Relationship


# Generic Exit Function.
def panic(data):
    print("\nPANIC: The program has Quit.\nInfo: {}".format(data))
    sys.exit()


# Data Stream Helpers
def find_tag(tag, file_stream):
    seek_limit = 1000 # Stop after X lines if tag not found.

    try:
        line = file_stream.readline()
    except:
        panic("Unable to read file_stream while scanning for tag.\nTag: {}\nLine Number: 0".format(tag))

    line_number = 0
    while tag not in line and line_number < seek_limit:
        try:
            line = file_stream.readline()
            line_number += 1
        except:
            panic("Unable to read file_stream while scanning for tag.\nTag: {}\nLine Number: {}".format(tag, line_number))

    return line, file_stream

def get_next_page(file_stream):
    line, file_stream = find_tag("<page>", file_stream)
    page_text = line
    while "</page>" not in line:
        try:
            line = file_stream.readline()
            page_text = page_text + line
        except:
            panic("Unable to read file_stream while gathering page_text.\nPage Text: {}".format(page_text))

    return page_text, file_stream


# XML Search Helpers
def get_id_and_redirect_status_from_page_text(text):
    parser = ET.XMLParser()
    parser.feed(text)
    root = parser.close()

    p_id = 0
    p_redirect = False

    for child in root.getchildren():
        if child.tag == "id":
            p_id = child.text
        if child.tag == "redirect":
            p_redirect = True

    return p_id, p_redirect

def get_id_and_redirect_status_and_title_from_page_text(text):
    parser = ET.XMLParser()
    parser.feed(text)
    root = parser.close()

    p_id = 0
    p_redirect = False
    p_title = ""

    for child in root.getchildren():
        if child.tag == "id":
            p_id = int(child.text)
        if child.tag == "redirect":
            p_redirect = True
        if child.tag == "title":
            p_title = child.text.replace(" ", "_").split("/")[0]

    return p_id, p_redirect, p_title

def get_wiki_links_from_text(text):
    start = "[["
    end = "]]"
    wiki_links = []
    link = '                                                    '
    text = text.replace("\n", "")

    if start in text:
        chunks = text.split(start)
        for chunk in chunks:
            if end in chunk:
                if "|" in chunk:
                    chunk = chunk.split("|")[0]
                if "#" in chunk:
                    chunk = chunk.split("#")[0]

                link = chunk.split(end)[0].replace(" ", "_")

                if "Project:AWB" != link:
                    wiki_links.append(link)

    return wiki_links

def get_page_title_from_page_text(text):
    parser = ET.XMLParser()
    parser.feed(text)
    root = parser.close()

    p_title = False

    for child in root.getchildren():
        if child.tag == "title":
            p_title = child.text.replace(" ", "_").split("/")[0]

    return p_title

def get_id_from_page_text(text):
    parser = ET.XMLParser()
    parser.feed(text)
    root = parser.close()

    p_id = ''

    for child in root.getchildren():
        if child.tag == "id":
            p_id = child.text

    return p_id

def get_redirect_status_from_page_text(text):
    parser = ET.XMLParser()
    parser.feed(text)
    root = parser.close()

    p_redirect = False

    for child in root.getchildren():
        if child.tag == "redirect":
            p_redirect = True

    return p_redirect

def get_redirect_status_from_page_parser(parser):
    p_redirect = False

    for child in parser.getchildren():
        if child.tag == "redirect":
            p_redirect = True

    return p_redirect

def get_id_from_page_parser(parser):
    p_id = ''

    for child in parser.getchildren():
        if child.tag == "id":
            p_id = child.text

    return p_id

def get_page_title_from_page_parser(parser):
    p_title = False
    for child in parser.getchildren():
        if child.tag == "title":
            p_title = child.text.replace(" ", "_").split("/")[0]
    return p_title



# Neo4J Helpers
def neo4j_connect(server_and_port, username, password):
    print("Connecting to neo4j...")
    try:
        authenticate(server_and_port, username, password)
        del password
        print("Successfully Connected!\n")
    except:
        del password
        error = "Failed to authenticate with neo4j server.\nServer: {}\nUsername: {}".format(server_and_port, username)
        panic(error)
    graph = Graph(server_and_port + '/db/data/')
    return graph

def neo4j_batch_create_nodes(graph, node_dict):
    try:
        tx = graph.begin()
        for n in node_dict:
            tx.create(node_dict[n])
        tx.commit()
    except:
        panic("Failed to commit batch to n4j.")

def neo4j_DELETE_ALL(graph):
    try:
        user_input = input("WARNING: THE PROGRAM IS ATTEMPTING TO DELETE ALL NODES FROM NEO4J.\nWARNING: DO YOU WISH TO CONTINUE? (y/n): ")
        if user_input == "y" or user_input == "Y":
            try:
                graph.delete_all()
                print("NODES AND REALATIONSHIPS DELETED...")
            except:
                panic("Attempted to delete neo4j graph items. This failed...")
        else:
            print("ABORTED...")
    except:
        panic("Attempted to delete neo4j graph items. This failed...")



# Disk Related Functions (save/load)
def save_dict_to_tsv(file_name, dictionary, silent=False):
    if not silent:
        print("Saving Dictionary Table [{}]...".format(file_name))
    save_dict_start_time = time.time()
    with open(file_name, 'w') as file_stream:
        for obj in dictionary:
            output = str(obj)
            # If our dictionary mutable object is a list, then we want to convert it to tsv string
            if isinstance(dictionary[obj], list):
                for i in range(0, len(dictionary[obj])):
                    output = output + "\t" + str(dictionary[obj][i])
            elif isinstance(dictionary[obj], tuple):
                for i in range(0, len(dictionary[obj])):
                    output = output + "\t" + str(dictionary[obj][i])
            output =  output + "\n"
            file_stream.write(output)
    if not silent:
        print("Saving Dictionary Table [{}] Complete...\nElapsed Time: {}s\n".format(file_name, "%.2f" % (time.time() - save_dict_start_time)))

def append_dict_to_tsv(file_name, dictionary, silent=False):
    if not silent:
        print("Saving Dictionary Table [{}]...".format(file_name))
    save_dict_start_time = time.time()
    with open(file_name, 'a') as file_stream:
        for obj in dictionary:
            output = str(obj)
            # If our dictionary mutable object is a list, then we want to convert it to tsv string
            if isinstance(dictionary[obj], list):
                for i in range(0, len(dictionary[obj])):
                    output = output + "\t" + str(dictionary[obj][i])
            elif isinstance(dictionary[obj], tuple):
                for i in range(0, len(dictionary[obj])):
                    output = output + "\t" + str(dictionary[obj][i])
            output =  output + "\n"
            file_stream.write(output)
    if not silent:
        print("Saving Dictionary Table [{}] Complete...\nElapsed Time: {}s\n".format(file_name, "%.2f" % (time.time() - save_dict_start_time)))

def load_pages_from_tsv(file_name):
    pages = {}
    with open(file_name, 'r') as file_stream:
        for line in file_stream:
            line = line.split("\n")[0]
            line = line.split("\t")
            wiki_id = int(line[1])
            redirect = bool(line[2])
            pages[line[0]] = (wiki_id, redirect)
    return pages

def load_links_from_tsv(file_name):
    links = {}
    with open(file_name, 'r') as file_stream:
        for line in file_stream:
            line = line.split("\n")[0]
            line = line.split("\t")
            links[line[0]] = line[1:]
    return links

def load_saved_tsv_files(pages_file, links_file):
    pages = load_pages_from_tsv(pages_file)
    # for obj in pages:
    #     print(obj, pages[obj])

    links = load_links_from_tsv(links_file)
    # for obj in links:
    #     print(obj, links[obj])

    return pages, links

def load_data_from_wiki_file(wiki_file, page_batch_size=1000, exit_after_first_batch=False, save_on_batch=False, pages_tsv_file_name="", links_tsv_file_name=""):
    # Define wiki_file data & dictionaries to store page data
    pages = {}
    links = {}
    nodes = {}

    # Step through wiki_file.
    print("Reading Pages from Wiki File...\t\tpage_batch_size: {}\texit_after_first_batch: {}\tsave_on_batch: {}".format(page_batch_size, exit_after_first_batch, save_on_batch))
    read_wiki_file_start_time = time.time()
    page_number = 0
    batch_number = 0
    with open(wiki_file, 'r') as wiki_file_stream:
        batch_start_time = time.time()
        done = False
        nodes_batch = {}
        while not done:
            page_number += 1

            # Get on each <page>...</page> and process data in <page>
            page_text, wiki_file_stream = get_next_page(wiki_file_stream)


            ##### Get Basic Data - Start #####
            ## Fast
            # parser = ET.XMLParser()
            # parser.feed(page_text)
            # root = parser.close()
            #
            # page_id = get_id_from_page_parser(root)
            # page_redirect = get_redirect_status_from_page_parser(root)
            # page_title = get_page_title_from_page_parser(root)

            ## Slower
            # page_id = get_id_from_page_text(page_text)
            # page_redirect = get_redirect_status_from_page_text(page_text)
            # page_title = get_page_title_from_page_text(page_text)

            ## Faster
            page_id, page_redirect, page_title = get_id_and_redirect_status_and_title_from_page_text(page_text)
            ##### Get Basic Data - End #####


            ##### Get Link Data - Start #####
            page_links = get_wiki_links_from_text(page_text)
            ##### Get Link Data - End #####


            ##### Store Data - Start #####
            pages[page_title] = (page_id, page_redirect)
            links[page_title] = page_links
            ##### Store Data - End #####


            if not save_on_batch:
                ##### Store neo4j Node - Start #####
                nodes_batch[page_title] = Node("Article", name=page_title, wiki_id=page_id, redirect=page_redirect)
                ##### Store neo4j Node - End #####


            # print("Page Nubmer: {}\tWiki Page ID: {}\tRedirect Page: {}\tNum of Links: {}".format(page_number, page_id, page_redirect, len(page_links)))


            # If we've collected as many pages as the page_batch_size, run a batch job.
            if page_number % page_batch_size == 0:
                # Run Batch job here. Also can function as a way to stop the program early at a given page number. (done = True)
                #       *worthy of note: it's assumed that not all of the data has been extracted from the wiki_file yet,
                #           so, we can't run batches that rely on knowing the parameters of every node. Those need to be run later.


                # neo4j_DELETE_ALL(neo4j_graph)
                # neo4j_batch_create_nodes(neo4j_graph, nodes_batch)

                nodes.update(nodes_batch)       # Add batch nodes to master nodes dictionary

                if save_on_batch:
                    print("\t\tSaving current batch...")
                    if pages_tsv_file_name == "" or links_tsv_file_name == "":
                        error = "User requested we save-on-batch, but, the function [load_data_from_wiki_file] did not receive a file name to save page or link data to.\n"
                        error = error + "Please specifiy pages_tsv_file_name & links_tsv_file_name.\nProvided Files:\npages_tsv_file_name: {}\nlinks_tsv_file_name: {}".format(pages_tsv_file_name, links_tsv_file_name)
                        panic(error)
                    else:
                        # Append pages dict to TSV
                        append_dict_to_tsv("data/pages.tsv", pages, silent=True)
                                                # Write links dict to TSV
                        append_dict_to_tsv("data/links.tsv", links, silent=True)

                        # Free up memory by clearing pages dict
                        pages = {}
                        # Free up memory by clearing links dict
                        links = {}


                if exit_after_first_batch:
                    print("\tBatch: {}\tComplete...\tTotal Elapsed Time: {}s\n\tExiting Readfile...".format(batch_number, "%.2f" % (time.time() - read_wiki_file_start_time)))
                    done = True
                else:
                    print("\tBatch: {}\tComplete...\tTotal Elapsed Time: {}s".format(batch_number, "%.2f" % (time.time() - read_wiki_file_start_time)))
                batch_number += 1
                batch_start_time = time.time()


    print("Read Wiki File Complete...\nElapsed Time: {}s\n".format("%.2f" % (time.time() - read_wiki_file_start_time)))
    return pages, links, nodes

def delete_file(file_name):
    print("Deleting File! ({})".format(file_name))
    with open(file_name, 'w') as file_stream:
        file_stream.write("")
    print("File Deleted! ({})\n".format(file_name))

def load_neo4j_profile(file_name):
    print("Loading neo4j profile...\tFile Name: {}".format(file_name))
    try:
        with open(file_name, 'r') as file_stream:
            line1 = file_stream.readline()
            line1 = line1.split("\n")[0]
            hostname, username = line1.split(",")
            file_stream.close()
        print("Sucessfully loaded profile!\n")
    except:
        panic("Failed to load profile...")
    return hostname, username

def save_neo4j_profile(hostname, username, file_name):
    print("Saving neo4j Profile...\tFile Name: {}".format(file_name))
    try:
        with open(file_name, 'w') as file_stream:
            text = hostname + "," + username
            file_stream.write(text)
            file_stream.close()
        print("Sucessfully saved profile!\n")
    except:
        panic("Failed to save profile...")





def main():
    start_time = time.time()
    print("Starting Program...\n")

    wiki_file = 'data/enwiki-20170820-pages-articles.xml'
    default_profile_file_name = "user/profile.csv"
    pages_tsv_file_name = "data/pages.tsv"
    links_tsv_file_name = "data/links.tsv"

    page_batch_size = 20000

    print("Would you like to connect to a neo4j server now?")
    user_input = input("Please enter (y) or (n): ")
    print()
    if user_input == "y":
        print("Use saved profile?")
        user_input = input("Please enter (y) or (n): ")
        print()
        if user_input == "y":
            server_and_port, username = load_neo4j_profile(default_profile_file_name)
            print("Loaded the following information:\nHostname: {}\nUsername: {}\n".format(server_and_port, username))
            print("Please provide a password. [eg: \"my#insecure#password123!!\"] (no quotes)\n(Use a secure password... It's just common sense.)")
            password = getpass.getpass("Enter Password: ")
            print()
            try:
                neo4j_graph = neo4j_connect(server_and_port, username, password)
                del password
            except:
                del password
                panic("Something unknown went wrong involving the neo4j connection process.")
        elif user_input == "n":
            print("Please provide a hostname and port. [eg: \"localhost:7474\"] (no quotes)")
            server_and_port = input("Enter Hostname: ")
            print()
            print("Please provide a username. [eg: \"neo4j\"] (no quotes)")
            username = input("Enter Username: ")
            print()
            print("Please provide a password. [eg: \"my#insecure#password123!!\"] (no quotes)\n(Use a secure password... It's just common sense.)")
            password = getpass.getpass("Enter Password: ")
            print()
            try:
                neo4j_graph = neo4j_connect(server_and_port, username, password)
                del password
                print("Would you like to save this Hostname & Username for future connections?")
                user_input = input("Please enter (y) or (n): ")
                print()
                if user_input == "y":
                    save_neo4j_profile(server_and_port, username, default_profile_file_name)
                elif user_input == "n":
                    pass
                else:
                    panic("Invalid user input [{}]".format(user_input))
            except:
                del password
                panic("Something unknown went wrong involving the neo4j connection process.")
            pass
        else:
            panic("Invalid user input [{}]".format(user_input))
    elif user_input == "n":
        pass
    else:
        panic("Invalid user input [{}]".format(user_input))


    print("Would you like to load data from wiki_file or from pre-built tsv?")
    print("1. wiki_file\t({})".format(wiki_file))
    print("2. tsv files\t({})({})".format(pages_tsv_file_name, links_tsv_file_name))
    user_input = input("Please Enter (1) or (2): ")
    print()

    if user_input == "1":
        print("Would you like to save data from wiki_file to tsv files?")
        user_input = input("Please Enter (y) or (n): ")
        print()

        if user_input == "y":
            print("Would you like to use the save-on-batch method? (Less RAM intensive as variables cleared after each batch written to file.)")
            print("\tIf program is inturrupted, files will be incomplete, but will contain prior batches data.")
            print("WARNING: If you type (y) or (n), existing TSV files will be deleted to avoid duplicate data!")
            user_input = input("Please Enter (y) or (n): ")
            print()

            if user_input == "y":
                delete_file(pages_tsv_file_name)
                delete_file(links_tsv_file_name)
                pages, links, nodes = load_data_from_wiki_file(wiki_file, page_batch_size=page_batch_size, exit_after_first_batch=False, save_on_batch=True, pages_tsv_file_name=pages_tsv_file_name, links_tsv_file_name=links_tsv_file_name )

            elif user_input == "n":
                delete_file(pages_tsv_file_name)
                delete_file(links_tsv_file_name)

                pages, links, nodes = load_data_from_wiki_file(wiki_file, page_batch_size=page_batch_size, exit_after_first_batch=False)

                # Write pages dict to TSV
                save_dict_to_tsv("data/pages.tsv", pages)
                # Write links dict to TSV
                save_dict_to_tsv("data/links.tsv", links)

        elif user_input == "n":
            pages, links, nodes = load_data_from_wiki_file(wiki_file, page_batch_size=page_batch_size, exit_after_first_batch=False)

        else:
            panic("Invalid user input [{}]".format(user_input))
    elif user_input == "2":
        print("Retrieving data from [{}] and [{}]...".format(pages_tsv_file_name, links_tsv_file_name))
        pages, links = load_saved_tsv_files(pages_tsv_file_name, links_tsv_file_name)

        print("Need to generate nodes. Code not yet implimented...")
        panic("Code not yet implimented to generate nodes from TSV loaded files.")
    else:
        panic("Invalid user input [{}]".format(user_input))




    # for page in pages:
    #     print(page, pages[page])
        # print("\tLinks:", links[page])

    print("Program Complete...\nTotal Elapsed Time: {}s".format("%.2f" % (time.time() - start_time)))






main()
