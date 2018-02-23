# Written by: Daniel Bowder
# Started on: 2018-01-15 -- 2018 Jan 15
# Github: https://github.com/DotBowder
#
# Version: 0.3.5
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

from py2neo import authenticate, Graph, Node, Relationship, NodeSelector
import mmap, time, sys
import getpass

def panic(data):
    print("\nPANIC: The program has Quit.\nInfo: {}".format(data))
    sys.exit()

def generate_page_master_list(mem_map, page_map_file='', stop_after=0, find_titles=True, find_links=False, print_tsv=False, save_tsv=False, return_list=True):
    open_term = b'<page>'
    close_term = b'</page>'
    open_term_length = len(open_term)
    close_term_length = len(close_term)

    page_list = []

    page_start = 0
    page_start = mem_map.find(open_term, page_start)
    page_number = 1

    if save_tsv:
        save_stream = open(page_map_file, 'w')

    while -1 != page_start:
        links = ''
        title = ''

        page_end = mem_map.find(close_term, page_start)
        if -1 != page_end:
            if find_titles:
                title = find_title(page_start, page_end + close_term_length, mem_map).replace("\n", "").replace(" ", "_")
            if find_links:
                links = find_links(page_start, page_end + close_term_length, mem_map)
            if print_tsv:
                print(str(page_start) + '\t' + str(page_end + close_term_length) + '\t' + title )
            if save_tsv:
                save_stream.write(str(page_start) + '\t' + str(page_end + close_term_length) + '\t' + title + '\n')
            if return_list:
                page_list.append((page_start, page_end + close_term_length, title, links))
            # print(title)
        page_number += 1
        if 0 != stop_after and page_number > stop_after:
            break
        page_start = mem_map.find(open_term, page_end + close_term_length)

    if save_tsv:
        save_stream.close()

    if return_list:
        return page_list

def load_tsv_page_master_list(page_map_file, stop_after=0):
    page_list = []

    with open(page_map_file, 'r') as file_stream:
        for line in file_stream:
            split_line = line.split('\n')[0].split('\t')
            page_list.append(  (int(split_line[0]), int(split_line[1]), split_line[2])  )
            if 0 != stop_after and len(page_list) >= stop_after:
                break
    return page_list

def generate_link_master_dict(mem_map, page_list, link_map_file='', print_tsv=False, save_tsv=False, return_list=True):
    link_master_dict = {}

    x = 0
    start_time = time.time()
    for page in page_list:
        links = find_links(page[0], page[1], mem_map)
        for link in links:
            link_master_dict[link] = 0
        x += 1
        if x % 177000 == 0:
            print("\tPage: {}\tElapsed Time: {}".format(x, "%.0f" % (time.time() - start_time)))


    if print_tsv:
        x = 0
        for link in link_master_dict:
            print(str(x) + '\t' + link)
            x += 1

    if save_tsv:
        x = 0
        with open(link_map_file, 'w') as save_stream:
            for link in link_master_dict:
                save_stream.write(str(x) + '\t' + link + '\n')
                x += 1

    if return_list:
        return link_master_dict

def load_tsv_link_master_dict(link_map_file, stop_after=0):
    link_dict = {}

    with open(link_map_file, 'r') as file_stream:
        for line in file_stream:
            split_line = line.split('\n')[0].split('\t')
            # print(split_line, line)
            if split_line != [""]:
                link_dict[split_line[1]] = split_line[0]
                if 0 != stop_after and len(link_dict) >= stop_after:
                    break
    return link_dict

def compress_link_data_with_link_master_dict(mem_map, page_list, link_dict, link_data_file='', save_tsv=False):
    pages_links_dict = {}

    x = 0
    start_time = time.time()
    for page in page_list:
        print(page)
        page_links_list = []
        for link in find_links(page[0], page[1], mem_map):
            try:
                page_links_list.append(link_dict[link])
            except:
                pass
        page_links_list = list(set(page_links_list))
        pages_links_dict[page[2]] = page_links_list
        x += 1
        if x % 177000 == 0:
            print("\Page: {}\tElapsed Time: {}\tEg: {}".format(x, "%.0f" % (time.time() - start_time), page_links_list))


    if save_tsv:
        x = 0
        with open(link_data_file, 'w') as save_stream:
            for page in pages_links_dict:
                output = page + '\t'
                for number in pages_links_dict[page]:
                    output = output + str(number) + '\t'
                save_stream.write(output[:-1])
                x += 1


    return pages_links_dict


def find_title(page_start, page_end, mem_map):
    open_term = b'<title>'
    close_term = b'</title>'
    open_term_length = len(open_term)
    close_term_length = len(close_term)

    title_start = mem_map.find(open_term, page_start)
    title_end = mem_map.find(close_term, page_start)
    return mem_map[title_start + open_term_length: title_end].decode('utf-8')

def find_links(page_start, page_end, mem_map):
    open_term = b'[['
    close_term = b']]'
    close_term_opt = b'|'
    open_term_length = len(open_term)
    close_term_length = len(close_term)

    links = []

    link_start = mem_map.find(open_term, page_start)

    while -1 != link_start and link_start < page_end:
        link_end = mem_map.find(close_term, link_start)
        opt_end = mem_map.find(close_term_opt, link_start)
        if -1 != opt_end and opt_end < link_end:
            link_end = opt_end
        # print("Acquired Link: {}".format(mem_map[link_start + open_term_length : link_end].replace(b' ', b'_').replace(b'\n', b'').decode('utf-8')))
        links.append(mem_map[link_start + open_term_length : link_end].replace(b' ', b'_').replace(b'\n', b'').decode('utf-8'))
        link_start = mem_map.find(open_term, link_end)


    return links


def neo4j_connect(server_and_port="", username="", password="", load_default_profile=True, default_profile_file_name=""):
    print("Connecting to neo4j...")

    if load_default_profile:
        server_and_port, username = load_neo4j_profile(default_profile_file_name)
        print("Loaded the following information:\nHostname: {}\nUsername: {}\n".format(server_and_port, username))
        print("Please provide a password. [eg: \"my#insecure#password123!!\"] (no quotes)\n(Use a secure password... It's just common sense.)")
        password = getpass.getpass("Enter Password: ")
        print()

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


def CLI_Continue(text):
    u_in = input(text)
    if "y" == u_in:
        return 1
    elif "n" == u_in:
        return 2
    else:
        return 0



class FileManager():
    def __init__(self, wiki_file, page_map_file, link_map_file, link_data_file):
        self.wiki_file = wiki_file
        self.page_map_file = page_map_file
        self.link_map_file = link_map_file
        self.link_data_file = link_data_file

    def Generate_And_Save_TSV_Page_Map(self, stop_after=0):
        with open(self.wiki_file, 'r+b') as f:
            with mmap.mmap(f.fileno(), 0) as mem_map:
                print("Generating TSV Page Map...")
                start_time = time.time()
                generate_page_master_list(mem_map, stop_after=stop_after, save_tsv=True, page_map_file=self.page_map_file, return_list=False)
                end_time = time.time()
                print("\tComplete...")
                print("\tTask Duration: {}s".format("%.0f" % (end_time - start_time)))

    def Generate_And_Save_TSV_Link_Map(self, page_list):
        # Link Map maps all of the links on all of the pages of wikipedia articles to a unique id.
        #   * Using my archive, I acquired 34,000,000 unique page links. This went up to 40 million after adding the non-duplicates in the page list.
        #   * This step does not yet check for duplicates in the page list.
        with open(self.wiki_file, 'r+b') as f:
            with mmap.mmap(f.fileno(), 0) as mem_map:
                print("Generating Link Map...")
                start_time = time.time()
                generate_link_master_dict(mem_map, page_list, save_tsv=True, link_map_file=self.link_map_file, return_list=False)
                end_time = time.time()
                print("\tComplete...")
                print("\tTask Duration: {}s".format("%.0f" % (end_time - start_time)))

    def Load_TSV_Page_Map(self, stop_after=0):
        # Page Map maps the page name to the 'mmap' character list object
        with open(self.page_map_file, 'r+b') as f:
            print("Loading Page Map...")
            start_time = time.time()
            page_list = load_tsv_page_master_list(self.page_map_file, stop_after=stop_after)
            end_time = time.time()
            print("\tComplete...")
            print("\tNumber of Pages: {}\n\tMemory Footprint of Page List: {}".format(len(page_list), sys.getsizeof(page_list)))
            print("\tTask Duration: {}s".format("%.0f" % (end_time - start_time)))
            return page_list

    def Load_TSV_Link_Map(self, stop_after=0):
        # Link Map maps a unique id to each link name           dict[page_name] = unique_id
        with open(self.link_map_file, 'r+b') as f:
            print("Loading Link Map...")
            start_time = time.time()
            link_dict = load_tsv_link_master_dict(self.link_map_file, stop_after=stop_after)
            end_time = time.time()
            print("\tComplete...")
            print("\tNumber of Links: {}\n\tMemory Footprint of Link List: {}".format(len(link_dict), sys.getsizeof(link_dict)))
            print("\tTask Duration: {}s".format("%.0f" % (end_time - start_time)))
            return link_dict

    def Compress_Link_Data_And_Save_TSV_Link_Data(self, page_list, link_dict):
        # Link Data is loaded into a dict where, the key is the name of the page, and the data is a list of numbers... dict[page_name] = [0, 56, 230431, 123, 23344]
        # Each number corresponds to a link name in the Link Map.
        with open(self.wiki_file, 'r+b') as f:
            with mmap.mmap(f.fileno(), 0) as mem_map:

                print("Creating Compressed Link Data File...")
                start_time = time.time()
                compress_link_data_with_link_master_dict(mem_map, page_list, link_dict, save_tsv=True, link_data_file=self.link_data_file)
                end_time = time.time()
                print("\tComplete...")
                print("\tTask Duration: {}s".format("%.0f" % (end_time - start_time)))

    def Get_Links_From_Wiki_File(self, page_start, page_end):
        # Returns list of links for a given page
        with open(self.wiki_file, 'r+b') as f:
            with mmap.mmap(f.fileno(), 0) as mem_map:
                return find_links(page_start, page_end, mem_map)





class ConnectionManager():
    def __init__(self, profile_dir="profiles/", profile_name="default", profile_ext=".profile", neo4j_graph_name="/db/data/"):

        self.connected = False
        self.profile_dir = profile_dir
        self.profile_ext = profile_ext
        self.neo4j_graph_name = neo4j_graph_name
        self.Load_Profile(profile_dir + profile_name + ".profile")
        print("Profile Data Loaded:\n\tHostname: {}\n\tUsername: {}".format(self.hostname, self.username))
        invalid = "That was an invalid response! Please type \"y\" or \"n\" without quotes.)"

        question = "Would you like to connect? Enter (y/n): "
        response = CLI_Continue(question)
        while 0 == response: # invlid response
            print(invalid)
            response = CLI_Continue(question)
        if response == 1: # y
            self.password = getpass.getpass("\nPlease type your password. (It's invisible)\neg:        this!BADpassword123   < ----- *that is actually a terrible password*\nType Here: ")
            self.Connect()
        elif response == 2: # n
            question_1 = "Would you like to enter a new hostname & username? Enter (y/n): "
            response_1 = CLI_Continue(question_1)
            while 0 == response_1: # invlid response
                print(invalid)
                response = CLI_Continue(question_1)
            if response_1 == 1: # y
                hostname_question = "Please type your hostname & port number.\neg:        localhost:7474\nType Here: "
                username_question = "Please type your username.\n       eg: neo4j\nType Here: "
                self.hostname = input(hostname_question)
                self.username = input(username_question)
                self.password = getpass.getpass("Please type your password. (It's invisible)\neg:        this!BADpassword123   < ----- *that is actually a terrible password*\nType Here: ")
                question_2 = "\nUsername: {}\nHostname: {}\nWould you like to continue? Enter (y/n): "
                response_2 = CLI_Continue(question_2)
                while 0 == response_2:
                    print(invalid)
                    response_2 = CLI_Continue(question_2)
                if response_2 == 1: # y
                    self.Connect()
                elif response_2 == 2: # n
                    self.connected = False
            elif response_1 == 2: # n
                self.connected = False
    # The Connection manager is in charge of connecting to and from the node4j server.
    # It can open a profile file (eg: default.profile)
    # The default profile is .../profiles/default.profile
    # It can connect to the neo4j server.
    # It can create realationships and nodes on the neo4js server using dictionaries and lists
    # It can open a transaction object, queue items into that transaction object, and then send the queued items to the server.
    #

    def Load_Profile(self, profile):
        print("Loading neo4j profile...\tFile Name: {}".format(profile))
        with open(profile, 'r') as file_stream:
            line1 = file_stream.readline()
            line1 = line1.split("\n")[0]
            self.hostname, self.username = line1.split(",")
            file_stream.close()
        if '' == self.hostname or '' == self.username:
            print("Failed to load profile information! Usetting default profile info instead. (localhost:7474, neo4j)")
            self.hostname = "localhost:7474"
            self.username =  "neo4j"
        print("Sucessfully loaded profile!\n")

    def Connect(self):
        print("\nInitiating server authentication... ")
        authenticate(self.hostname, self.username, self.password)
        try:
            del self.password
            self.connected == True
            print("Attempting to retreive server Graph...")
            self.graph = Graph(self.hostname + self.neo4j_graph_name)
            print("\tGraph Acquired!\n")
            self.node_selector_single = NodeSelector(self.graph)
        except:
            del self.password
            self.connected == False
            print("Failed!")

    def Find_Node_By_Name(self, name, node_type="Article"):
        # Use our FileManager.node_selector_single we created from our graph after signing into the server for the first time..
        return self.node_selector_single.select(node_type, name=name).limit(1).first()

    def Create_Relationships_On_Server_From_Page_List_And_Link_Dict(self, page_list, file_manager, batch_size=1000):
        # Open a neo4j transaction using the file-manager self functions (Transaction_open, Transaction_...)
        # Iterate through each node provided by the page_list. For each page, find the neo4j node with it's name with Find_Node_By_Name(), and store the name in "node_a"
        # Use the file_manager to extract the link_list for the current page for you, with Get_Links_From_Wiki_File(start_char#, end_char#)
        # For each link on this page, find the neo4j node with it's name with Find_Node_By_Name(), and store the name in "node_b"
        # Create a relationship "LINKSTO" frmo node_a, to node_b and add it to the Transaction queue with Transaction_Add_Create_Node(node_a, node_b)
        # Then, check if we've reached our batch_size, and if we have, send the queue to the server, and print some statistics for us.

        start_time = time.time()
        batch_start = time.time()
        self.Transaction_Open()

        x = 1
        page_number = 1
        batch_count = 1
        number_of_pages = len(page_list)
        for page in page_list:
            search_time = time.time()
            node_a = self.Find_Node_By_Name(page[2])
            # print("Search Duration: {}\tSearch of {}".format("%.3f" % (time.time() - search_time), page[2]))
            page_link_list = file_manager.Get_Links_From_Wiki_File(page[0], page[1])
            for link in page_link_list:
                search_time = time.time()
                node_b = self.Find_Node_By_Name(link)
                # print("\tSearch Duration: {}\tSearch of {}".format("%.3f" % (time.time() - search_time), link))
                # print("\tB:", link, node_b)
                self.Transaction_Add_Create_Relationship(node_a, node_b, relationship="LINKSTO")
                if x % batch_size == 0 or page_number == number_of_pages:
                    # panic("Development STOP")
                    self.Transaction_Close()
                    batch_end = time.time()
                    batch_count += 1
                    print("Batch# {}\tRelationship# {}\tBatch Duration: {}s\tAvg Relationships per Second: {}".format(batch_count, x, "%.0f" % (batch_end - batch_start), "%.0f" % (x/(batch_end - start_time))))
                    batch_start = time.time()
                    self.Transaction_Open()
                x += 1
            page_number += 1

    def Create_Nodes_On_Server_From_Name_Keyed_Dict(self, node_dict, batch_size=1000):
        # Open a neo4j transaction using the file-manager self functions (Transaction_open, Transaction_...)
        # Iterate through each node provided by the node_dict. The key of the dict is the string variable of the node's name.
        # Add a new node to the neo4j transaction queue, passing the new node's name in with node_name.
        # Then, check if we've reached our batch_size, and if we have, send the queue to the server, and print some statistics for us.

        limit = len(node_dict)
        start_time = time.time()
        batch_start = time.time()
        self.Transaction_Open()
        for node_name in node_dict:
            self.Transaction_Add_Create_Node(node_name)
            if x % batch_size == 0 or x == limit:
                self.Transaction_Close()
                batch_end = time.time()
                batch_count += 1
                print("Batch# {}\tLink# {}\tBatch Duration: {}s\tAvg Links per Second: {}".format(batch_count, x, "%.0f" % (batch_end - batch_start), "%.0f" % (x/(batch_end - start_time))))
                batch_start = time.time()
                self.Transaction_Open()
            x += 1


        # x = 0
        # batch_size = 1000
        # batch_count = 0
        # cm.Transaction_Open()
        # start_time = time.time()
        # batch_start = start_time
        # for url in link_dict:
        #     cm.Transaction_Add_Create_Node(url)
        #
        #     x += 1
        #     if x % batch_size == 0:
        #         cm.Transaction_Close()
        #         batch_end = time.time()
        #         batch_count += 1
        #         print("Batch# {}\tLink# {}\tBatch Duration: {}s\tAvg Links per Second: {}".format(batch_count, x, "%.0f" % (batch_end - batch_start), "%.0f" % (x/(batch_end - start_time))))
        #         batch_start = time.time()
        #         cm.Transaction_Open()

    def Transaction_Open(self):
        # Open a neo4js Graph Transaction   ## http://py2neo.org/v3/database.html#py2neo.database.Transaction
        self.tx = self.graph.begin()

    def Transaction_Add_Create_Node(self, name, node_type="Article"):'
        # Queue CREATE Node in Transaction.   ## http://py2neo.org/v3/database.html#py2neo.database.Transaction
        self.tx.create(Node(node_type, name=name))

    def Transaction_Add_Create_Relationship(self, node_a, node_b, relationship="LINKSTO"):
        ## Queue CREATE Relationship in Transaction.  # http://py2neo.org/v3/database.html#py2neo.database.Transaction
        self.tx.create(Relationship(node_a, relationship, node_b))

    def Transaction_Close(self):
        # Commit a neo4js Graph Transaction ## http://py2neo.org/v3/database.html#py2neo.database.Transaction
        self.tx.commit()


wiki_file = 'data/enwiki-20170820-pages-articles.xml'
page_map_file = 'data/page_map.tsv'
link_map_file = 'data/link_map.tsv'
link_data_file = 'data/link_data.tsv'

fm = FileManager(wiki_file, page_map_file, link_map_file, link_data_file)
cm = ConnectionManager()

# Step 1. Generate Page and Link lookup tables to facilitate further transactions
fm.Generate_And_Save_TSV_Page_Map()
fm.Generate_And_Save_TSV_Link_Map(page_list)

# Step 2. Load the page and link lookup tables to be processed
page_list = fm.Load_TSV_Page_Map()
link_dict = fm.Load_TSV_Link_Map()

# Step 3. Combine Page list and link_dict to one list & generate nodes for each article.
#
# The link dict currently contains objects that have been linked to from a page.
# The link dict does NOT currently contain ALL of the pages themselves, it does however, contain some.
# Here, we check and see if the link_dict contains the title of the page. If the link_dict does contain
# the title, then, we HIT the dictionary obeject, and we set the value to be 0. Links that are also pages,
# will have a link_id of 0. When we MISS the dictionary object, and do not find an entry, we create an
# entry in the link_dict using the page title as the key.
hit = 0
miss = 0
for page in page_list:
    try:
        junk = link_dict[page[2]]
        link_dict[page[2]] = 0
        hit += 1
    except:
        link_dict[page[2]] = 0
        miss += 1
print("Hits: {}\tMisses: {}\nLength of Link Dict: {}".format(hit, miss, len(link_dict)))

cm.Create_Nodes_On_Server_From_Name_Keyed_Dict(link_dict)


# Step 4. Iterate through the page_list and generate relationships for each page and their destination nodes.
cm.Create_Relationships_On_Server_From_Page_List_And_Link_Dict(page_list, fm)
