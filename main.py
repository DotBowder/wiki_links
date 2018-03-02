# Written by: Daniel Bowder
# Github: https://github.com/DotBowder

import WIKI_LINK_PARSE
import WIKI_LINK_NEOCONNECT
import os, sys

current_directory = os.getcwd() + "/"
wiki_file = "localdisk/enwiki-20170820-pages-articles.xml"
wiki_reduced_file = "localdisk/wiki_reduced_file.tsv"

master_ids_file = "localdisk/master_ids.tsv"
relationships_file = "localdisk/relationships.tsv"

def panic(data):
    print("\nPANIC: The program has Quit.\nInfo: {}".format(data))
    sys.exit()


def Print_Program_Info():
    print("///////////////////////////////////////////////////////////////////////////////////////////")
    print("///                                                                                     ///")
    print("///                      WIKI LINK NEO4J DATA IMPORT TOOL                               ///")
    print("///                      Version: 0.6                                                   ///")
    print("///                      Developed by: Daniel Bowder                                    ///")
    print("///                      https://github.com/DotBowder/wiki_links                        ///")
    print("///                                                                                     ///")
    print("///////////////////////////////////////////////////////////////////////////////////////////")
    print("")

def Print_Licence():
    print("///////////////////////////////////////////////////////////////////////////////////////////")
    print("///                                                                                     ///")
    print("///   MIT License                                                                       ///")
    print("///                                                                                     ///")
    print("///   Copyright (c) 2018 Daniel Bowder                                                  ///")
    print("///   Permission is hereby granted, free of charge, to any person obtaining a copy      ///")
    print("///   of this software and associated documentation files (the \"Software\"), to deal     ///")
    print("///   in the Software without restriction, including without limitation the rights      ///")
    print("///   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell         ///")
    print("///   copies of the Software, and to permit persons to whom the Software is             ///")
    print("///   furnished to do so, subject to the following conditions:                          ///")
    print("///                                                                                     ///")
    print("///   The above copyright notice and this permission notice shall be included in all    ///")
    print("///   copies or substantial portions of the Software.                                   ///")
    print("///                                                                                     ///")
    print("///   THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR        ///")
    print("///   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,          ///")
    print("///   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE       ///")
    print("///   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER            ///")
    print("///   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,     ///")
    print("///   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE     ///")
    print("///   SOFTWARE.                                                                         ///")
    print("///                                                                                     ///")
    print("///////////////////////////////////////////////////////////////////////////////////////////")
    print("")

def Print_Process():
    print("///////////////////////////////////////////////////////////////////////////////////////////")
    print("///                                                                                     ///")
    print("///   PROCCESS                                                                          ///")
    print("///                                                                                     ///")
    print("///////////////////////////////////////////////////////////////////////////////////////////")
    print("///                                                                                     ///")
    print("///   There are three file preparation stages:                                          ///")
    print("///                                                                                     ///")
    print("///    1. Extract Titles and Links from wiki_file and save to wiki_reduced_file.tsv     ///")
    print("///    2. Generate Master IDs for each page, and save to master_ids.tsv                 ///")
    print("///    3. Generate Relationships for each page, and save to relatiohships.tsv           ///")
    print("///                                                                                     ///")
    print("///   There are three neo4j import stages:                                              ///")
    print("///                                                                                     ///")
    print("///    1. The neo4j graph needs to index names and IDs for speedy lookup.               ///")
    print("///        CREATE CONSTRAINT ON (n:Article) ASSERT n.name is UNIQUE;                    ///")
    print("///        CREATE CONSTRAINT ON (n:Article) ASSERT n.id is UNIQUE;                      ///")
    print("///                                                                                     ///")
    print("///    2. master_ids.tsv can be used to create all Nodes                                ///")
    print("///        USING PERIODIC COMMIT                                                        ///")
    print("///        LOAD CSV FROM 'file:///master_ids.tsv' AS line FIELDTERMINATOR '\\t'          ///")
    print("///        CREATE (:Article {name: line[0], id: line[1]})                               ///")
    print("///                                                                                     ///")
    print("///    3. relatiohships.tsv can be used to merge Relationships with Nodes               ///")
    print("///        USING PERIODIC COMMIT                                                        ///")
    print("///        LOAD CSV FROM 'file:///relationships.tsv' AS line FIELDTERMINATOR '\\t'       ///")
    print("///        MATCH (source:Article {id: line[0] })                                        ///")
    print("///        MATCH (dest:Article {id: line[1] })                                          ///")
    print("///        MERGE (source)-[:LINKSTO{strength: line[2]}]->(dest)                         ///")
    print("///                                                                                     ///")
    print("///////////////////////////////////////////////////////////////////////////////////////////")
    print("")

def Print_Help():
    print("")
    print("")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("////                                                                                               ////")
    print("//// Wikipedia distributes archives of wikipedia articles.                                         ////")
    print("//// A Torrent download for these archives can be found here:                                      ////")
    print("////      https://meta.wikimedia.org/wiki/Data_dump_torrents                                       ////")
    print("////                                                                                               ////")
    print("//// This program chops up a wikipedia archive and extracts artcile titles and links to other      ////")
    print("//// wikipedia articles. Data extracted from the wikipedia archive will be imported to neo4j for   ////")
    print("//// query or further analysis.                                                                    ////")
    print("////                                                                                               ////")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("")
    print("")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("//// The archive file I used was (enwiki-20170820-pages-articles.xml) and the first 10 lines look like the following          ////")
    print("//// $ head -n 10 enwiki-20170820-pages-articles.xml                                                                          ////")
    print("//// eg:                                                                                                                      ////")
    print("////    <mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.10/\" ...   ...   ... >                                        ////")
    print("////        <siteinfo>                                                                                                        ////")
    print("////            <sitename>Wikipedia</sitename>                                                                                ////")
    print("////            <dbname>enwiki</dbname>                                                                                       ////")
    print("////            <base>https://en.wikipedia.org/wiki/Main_Page</base>                                                          ////")
    print("////            <generator>MediaWiki 1.30.0-wmf.14</generator>                                                                ////")
    print("////            <case>first-letter</case>                                                                                     ////")
    print("////            <namespaces>                                                                                                  ////")
    print("////                <namespace key=\"-2\" case=\"first-letter\">Media</namespace>                                                 ////")
    print("////                <namespace key=\"-1\" case=\"first-letter\">Special</namespace>                                               ////")
    print("////                                                                                                                          ////")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("")
    print("")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("////                                                                                               ////")
    print("//// This program chops up the xml wikipedia archive file above into a \"wiki reduced file.\"        ////")
    print("//// This wiki reduced file is a TSV type file which pulls the Title of a page,                    ////")
    print("//// and the Links to other wikipeidia articles, contained by the source article.                  ////")
    print("////                                                                                               ////")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("//// The first 10 lines of wiki_reduced_file.tsv look like the following...                                                   ////")
    print("//// $ head -n 10 wiki_reduced_file.tsv                                                                                       ////")
    print("//// eg:                                                                                                                      ////")
    print("////    T	AccessibleComputing                                                                                           ////")
    print("////    L	Template:This_is_a_redirect                                                                                   ////")
    print("////    L	Template:Redirect_category_shell                                                                              ////")
    print("////    L	Computer_accessibility                                                                                        ////")
    print("////    T	Anarchism                                                                                                     ////")
    print("////    L	Special:Contributions/2601:204:C404:27D2:9C42:2F6F:4B2D:1EDD                                                  ////")
    print("////    L	political_philosophy                                                                                          ////")
    print("////    L	self-governance                                                                                               ////")
    print("////    L	stateless_society                                                                                             ////")
    print("////    L	Hierarchy                                                                                                     ////")
    print("////                                                                                                                          ////")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("")
    print("")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("////                                                                                               ////")
    print("//// Next, the program extracts unique Article & Link names combind and assigns unique ids.        ////")
    print("//// A TSV file called master_ids.tsv is saved to record this unique ID table.                     ////")
    print("////                                                                                               ////")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("//// The first 10 lines of master_ids.tsv look like the following...                                                          ////")
    print("//// $ head -n 10 master_ids.tsv                                                                                              ////")
    print("//// eg:                                                                                                                      ////")
    print("////     AccessibleComputing	0                                                                                                ////")
    print("////     Template:This_is_a_redirect	1                                                                                        ////")
    print("////     Template:Redirect_category_shell	2                                                                                ////")
    print("////     Computer_accessibility	3                                                                                            ////")
    print("////     Anarchism	4                                                                                                        ////")
    print("////     Special:Contributions/2601:204:C404:27D2:9C42:2F6F:4B2D:1EDD	5                                                    ////")
    print("////     political_philosophy	6                                                                                            ////")
    print("////     self-governance	7                                                                                                    ////")
    print("////     stateless_society	8                                                                                                ////")
    print("////     Hierarchy	9                                                                                                        ////")
    print("////                                                                                                                          ////")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("")
    print("")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("////                                                                                               ////")
    print("//// Next, the program extracts unique Article & Link names combind and assigns unique ids.        ////")
    print("//// A TSV file called master_ids.tsv is saved to record this unique ID table.                     ////")
    print("////     TSV Format: [  source_id \t dest_id \t strength  ]                                        ////")
    print("////      *where strength is the number of times a source article has linked to this destination.  ////")
    print("////                                                                                               ////")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("//// The first 10 lines of master_ids.tsv look like the following...                                                          ////")
    print("//// $ head -n 10 master_ids.tsv                                                                                              ////")
    print("//// eg:                                                                                                                      ////")
    print("////     0	1	1                                                                                                            ////")
    print("////     0	2	1                                                                                                            ////")
    print("////     0	3	1                                                                                                            ////")
    print("////     4	5	1                                                                                                            ////")
    print("////     4	6	1                                                                                                            ////")
    print("////     4	7	1                                                                                                            ////")
    print("////     4	8	1                                                                                                            ////")
    print("////     4	9	1                                                                                                            ////")
    print("////     4	10	1                                                                                                            ////")
    print("////     4	11	5                                                                                                            ////")
    print("////                                                                                                                          ////")
    print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("")
    print("")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("////                                                                                               ////")
    print("//// The master_ids.tsv and relationships.tsv files can be used to import data into neo4j.         ////")
    print("//// Example Cypher Querry:                                                                        ////")
    print("////       CREATE CONSTRAINT ON (n:Article) ASSERT n.name is UNIQUE;                               ////")
    print("////       CREATE CONSTRAINT ON (n:Article) ASSERT n.id is UNIQUE;                                 ////")
    print("////                                                                                               ////")
    print("////       USING PERIODIC COMMIT                                                                   ////")
    print("////       LOAD CSV WITH HEADERS FROM 'file:///master_ids.tsv' AS line FIELDTERMINATOR '\t'        ////")
    print("////       CREATE (:Article {name: line[0], id: line[1]})                                          ////")
    print("////                                                                                               ////")
    print("////       USING PERIODIC COMMIT                                                                   ////")
    print("////       LOAD CSV FROM 'file:///relationships.tsv' AS line FIELDTERMINATOR '\t'                  ////")
    print("////       MATCH (source:Article {id: line[0] })                                                   ////")
    print("////       MATCH (dest:Article {id: line[1] })                                                     ////")
    print("////       MERGE (source)-[:LINKSTO{strength: line[2]}]->(dest)                                    ////")
    print("////                                                                                               ////")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("")
    print("")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")
    print("////                                                                                               ////")
    print("////       Python impol            ////")
    print("////                                                                                               ////")
    print("///////////////////////////////////////////////////////////////////////////////////////////////////////")





def Check_Files():
    print("Checking to see what data files already exist...\n")

    # Wiki File
    if os.path.isfile(wiki_file):
        print('\tWiki file....\t\texists!\t\t{}'.format(wiki_file))
    else:
        print('\tWiki file does not exist!')
        print("\tWARNING\tMissing File:\t" + current_directory + wiki_file + "\n\t\tWikipedia Data Dump Torrents\n\t\thttps://meta.wikimedia.org/wiki/Data_dump_torrents\n\t\tenwiki-[date]-pages-articles.xml.bz2")
    if os.path.isfile(wiki_reduced_file):
        print('\tWiki Reduced file....\texists!\t\t{}'.format(wiki_reduced_file))
    else:
        print('\tWiki Reduced file does not exist!\t\t{}'.format(wiki_reduced_file))
        if os.path.isfile(wiki_file):
            print("")
            user_input = input('Would you like to generate a wiki reduced file by extracting titles and links from the wiki data file?\nEnter (y/n): ')
            if "y" == user_input:
                WIKI_LINK_PARSE.Reduce_Wiki_Datastore(wiki_file, wiki_reduced_file)
                print("Created Master IDs file!")
            elif "n" == user_input:
                pass
            else:
                panic("Invalid Input!")



    # Master ID TSV File (lookup table)
    if os.path.isfile(master_ids_file):
        print('\tMaster IDs file....\texists!\t\t{}'.format(master_ids_file))
    else:
        print('\tMaster IDs file does not exist!\t\t{}'.format(master_ids_file))
        if os.path.isfile(wiki_file) and os.path.isfile(wiki_reduced_file):
            print("")
            user_input = input('Would you like to generate a master ids file by extracting titles and links from the reduced wiki data file?\nEnter (y/n): ')
            if "y" == user_input:
                WIKI_LINK_PARSE.Save_Node_IDs(wiki_reduced_file, master_ids_file)
                print("Created Master IDs file!")
            elif "n" == user_input:
                pass
            else:
                panic("Invalid Input!")
        elif os.path.isfile(wiki_file) and not os.path.isfile(wiki_reduced_file):
            print("You must create a Reduced Wiki Datafile before generating a Master IDs file.")
            print("Please generate a reduced wiki file.")


    # Relationship's TSV File
    if os.path.isfile(relationships_file):
        print('\tRelationships file....\texists!\t\t{}'.format(relationships_file))
    else:
        print('\tRelationships file does not exist!\t{}'.format(relationships_file))
        if os.path.isfile(wiki_file) and os.path.isfile(wiki_reduced_file):
            print("")
            user_input = input('Would you like to generate a Relationships file by extracting titles and links from the reduced wiki data file?\nEnter (y/n): ')
            if "y" == user_input:
                WIKI_LINK_PARSE.Save_Relationships(wiki_reduced_file, master_ids_file, relationships_file)
                print("Created Relationships file!")
            elif "n" == user_input:
                pass
            else:
                panic("Invalid Input!")
        elif os.path.isfile(wiki_file) and not os.path.isfile(wiki_reduced_file):
            print("You must create a Reduced Wiki Datafile before generating a Relationship file.\nPlease generate a reduced wiki file.")
        elif os.path.isfile(wiki_file) and not os.path.isfile(master_ids_file):
            print("You must create a Master IDs file before generating a Relationship file.\nPlease generate a Master IDs file.")


def Overwrite_Files():
    print("Would you like to overwrite a file?")
    print("1. Wiki Reduced File     (must re-generate master_ids & relationships if this file is changed)")
    print("2. Master IDs File       (must re-generate relationships if this file is changed)")
    print("3. Relationships File    (this is the file file to be generated before neo4j import)")
    print("n. -- Go Back!")
    print("h. -- Help")
    user_input = input("Please Enerter (1/2/3/h): ")
    print("")
    if 'h' == user_input:
        Print_Help()
    elif 'n' == user_input:
        pass
    elif '3' == user_input:
        WIKI_LINK_PARSE.Save_Relationships(wiki_reduced_file, master_ids_file, relationships_file)
    elif '2' == user_input:
        WIKI_LINK_PARSE.Save_Node_IDs(wiki_reduced_file, master_ids_file)
    elif '1' == user_input:
        WIKI_LINK_PARSE.Reduce_Wiki_Datastore(wiki_file, wiki_reduced_file)


def Transmit_Data(hostname="bolt://localhost", username="neo4j", password="mysillypassword"): # Don't use a password like that....
    neo = WIKI_LINK_NEOCONNECT.NEO4J_CONNECT(hostname, username, password)
    neo.Setup_Constraints()

    print("What data would you like to submit?")
    print("1. Nodes")
    print("2. Relationships")
    print("h. -- Help")
    user_input = input("Please Enerter (1/2/h): ")
    print("")
    if 'h' == user_input:
        Print_Help()
    elif '2' == user_input:
        neo.Create_Relationships()
    elif '1' == user_input:
        neo.Create_Nodes()
    neo.Close()


def Root_Menu():
    print("")
    print("What would you like to do?")
    print("1. Check that my neo4j tsv files have been generated.")
    print("2. Overwrite an existing tsv file with a new replacement.")
    print("3. Send tsv file data to neo4j server.")
    print("h. -- Help")
    user_input = input("Please Enerter (1/2/3/h): ")
    print("")
    if 'h' == user_input:
        Print_Help()
    elif '3' == user_input:
        Transmit_Data(hostname="bolt://localhost", username="neo4j", password="mysillypassword")
    elif '2' == user_input:
        Overwrite_Files()
    elif '1' == user_input:
        Check_Files()


Print_Program_Info()
Print_Licence()
while True:
    Root_Menu()

# Overwrite_Files()
# Print_Program_Info()
# Print_Licence()
# Print_Process()
# Check_Files()
