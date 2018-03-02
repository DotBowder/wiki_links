# Written by: Daniel Bowder
# Github: https://github.com/DotBowder

import time

# Functions to extract relevent data from wikipedia data store, and write data to files.
# master_ids_file is a lsit of all of the Nodes for this neo4j graph.
# relationships_file is a lsit of all of the node to node LINKSTO relationships.

def panic(data):
    print("\nPANIC: The program has Quit.\nInfo: {}".format(data))
    sys.exit()

def Reduce_Wiki_Datastore(wiki_file, wiki_reduced_file, print_batch=177000):
    print("Extracting Page Titles and Links from Wiki Datastore...\n")
    page_number = 1
    start_time = time.time()
    batch_time = start_time
    with open(wiki_file, 'r') as file_stream:
        with open(wiki_reduced_file, 'w') as save_stream: # Save Page Titles and Links to wiki_reduced_file
            for line in file_stream:
                if '<page>' in line:
                    if page_number % print_batch == 0:
                        print("Page: {}\tTime: {}\tPages per Second: {}".format(page_number, ("%.2f" % (time.time() - start_time)), "%.0f" % (print_batch / (time.time() - batch_time))))
                        batch_time = time.time()
                elif '</page>' in line:
                    page_number += 1
                elif '<title>' in line:
                    t = line.split('<title>')[1].split('</title>')[0].replace('\"','\\"').replace('&quot;','\\"').replace('&amp;','&').replace('&nbsp;','_').replace(' ', '_').replace('\t', '')
                    if ';' not in t and '{' not in t and '}' not in t and '`' not in t and '\\' not in t:
                        save_stream.write('T\t' + t + '\n')
                elif '[[' in line:
                    link_list = line.split('[[')[1:]
                    for link in link_list:
                        l = link.split(']]')[0].split('|')[0].replace('\"','\\"').replace('&quot;','\\"').replace('&amp;','&').replace('&nbsp;','_').replace(' ', '_').replace('\t', '')
                        if ';' not in l and '{' not in l and '}' not in l and '`' not in l and '\\' not in l:
                            save_stream.write( 'L\t' + l + '\n')
    print("Extraction Complete!\nDuration: {}".format( "%.2f" % (time.time() - start_time)))

def Save_Node_IDs(wiki_reduced_file, master_ids_file, print_batch=400000):
    print("Saving Master ID Table...\n")

    master_ids = {}
    link_counts = {}

    start_time = time.time()
    current_page = ""

    print("\tSaving lookup table for Page IDs / Destination IDs...\n")
    with open(wiki_reduced_file, 'rb') as file_stream:
        with open(master_ids_file, 'w') as save_stream:

            page_number = 0
            id_count = 0

            for line in file_stream:
                if b'\n' != line:
                    l = line.split(b'\n')[0].split(b'\t')
                    try:
                        break_check = master_ids[l[1]]
                        link_counts[l[1]] += 1
                    except:
                        master_ids[l[1]] = 0
                        link_counts[l[1]] = 1
                        save_stream.write(l[1].decode('utf-8') + '\t' + str(id_count) + '\n')
                        id_count += 1
                        if id_count % print_batch == 0:
                            print("\tID # {}\tLast Record: {}".format(id_count, l))

    print("Table Saved!\nDuration: {}".format( "%.2f" % (time.time() - start_time)))

def Save_Relationships(wiki_reduced_file, master_ids_file, relationships_file, print_batch=177000):
    print("Saving Relationships File...\n")
    current_page_dest_id_strength = {}
    master_ids = {}
    page_ids = {}
    dest_ids = {}

    start_time = time.time()
    current_page = ""
    page_number = 0
    link_number = 0

    print("\tLoading Master IDs File...\n")
    with open(master_ids_file, 'r') as read_stream:
        for line in read_stream:
            l = line.replace('\n', '').split('\t')
            master_ids[l[0]] = l[1]


    print("\tSaving Relationships File...\n")
    with open(wiki_reduced_file, 'r') as read_stream:
        with open(relationships_file, 'w') as save_stream:
            # save_stream.write("source_id\tdest_id\tstrength\n")
            current_page = ""
            output = ""
            for line in read_stream:
                if '\n' != line:
                    if 'L' == line[0]:
                        try:
                            current_page_dest_id_strength[line[2:-1]] += 1
                        except:
                            current_page_dest_id_strength[line[2:-1]] = 1
                        link_number += 1
                    elif 'T' == line[0]:
                        for dest in current_page_dest_id_strength:
                            save_stream.write( current_page + "\t" + master_ids[dest] + "\t" + str(current_page_dest_id_strength[dest]) + "\n" )
                        current_page_dest_id_strength = {}
                        current_page = master_ids[line[2:-1]]
                        page_number += 1


    print("Relationships Saved!\nDuration: {}".format( "%.2f" % (time.time() - start_time)))

def Count_Node_Strength(master_ids_file, relationships_file):
    pass
    # To be filled in at a later date.

if __name__ == "__main__":

    print_batch = 177000

    wiki_file = "data/enwiki-20170820-pages-articles.xml"
    wiki_reduced_file = "data/wiki_reduced_file.tsv"
    master_ids_file = "data/master_ids.tsv"
    relationships_file = "data/relationships.tsv"

    # 1.
    Reduce_Wiki_Datastore(wiki_file, wiki_reduced_file)

    # 2.
    Save_Node_IDs(wiki_reduced_file, master_ids_file)

    # 3.
    Save_Relationships(wiki_reduced_file, master_ids_file, relationships_file)
