# Written by: Daniel Bowder
# Started on: 2018-01-15 -- 2018 Jan 15
# Github: https://github.com/DotBowder
#
#
# This program takes in the XML, enwiki wikimedia data dump for Wikipedia pages.
# Located Here: https://meta.wikimedia.org/wiki/Data_dump_torrents
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
#
# This program uses lxml to parse the xml file, and extract useful information such as
# website links, titles, and other information located on a wikipedia page.
#
# This program, currently, makes a full pass of the data file, to generate a lookup file
# referencing the start and end lines of each XML <page> tag. This effectivly chops the
# ingress data file into managable page chunks. The full pass isn't necessary if a lxml
# prase object was fed each line, but, this is what made sense to me initially, before
# exploring lxml deeper. This lxml parser may be implimented at a later date.
# See: http://lxml.de/parsing.html




import time
from lxml import etree as ET
from subprocess import Popen, PIPE


data_file = 'data/enwiki-20170820-pages-articles.xml'
lookup_file = 'data/pagebreaks.txt'

def panic(data):
    print("ERROR: The program has failed.\nInfo: {}".format(data))

def parse_page_lines(data_file, lookup_file):
    lineNumber = 0
    pageNumber = 0
    with open(data_file, 'r') as data_steam:
        with open(lookup_file, 'w') as lookup_stream:
            start_line = 0
            for data_line in data_steam:
                if '<page>' in data_line:
                    start_line = lineNumber
                if '</page>' in data_line:
                    lookup_stream.write(str(start_line)+","+str(lineNumber)+"\n")
                    pageNumber += 1
                lineNumber += 1

# start_time = time.time()
# parse_page_lines(data_file, lookup_file) # Use with tee or to write data to file.
# print("Elapsed Time: {}s".format("%.0f" % (time.time() - start_time)))



def step_through_pages(data_file, lookup_file):
    # Each line in the lookup file defines a new xml page tag
    # <page> ..... </page>
    # lookup_file format:  [<page> start line],[</page> end line]

    # Because each line in the lookup file defines a <page></page>, we can
    # step through each xml page tag in our giant xml data file

    # Open Lookup File
    with open(lookup_file, 'r') as lookup_stream:
        # Open Data File
        with open(data_file, 'r') as data_stream:

            # We start on line 0 in the data file
            dataLineNumber = 0
            # We need a variable to track what page we're on. (The line number of the lookup file that we're on.)
            pageNumber = 0

            # Step through each line of the lookup file
            for lookup_line in lookup_stream:


                # pageText will contain the full XML string of this <page></page>
                # We will eventually run pageText through an HTML/XML parser.
                pageText = ""

                # dataLine is a temporary variable to hold the current line of our data file.
                dataLine = ""

                # Here, we take our current looup table line, and extract the page
                # start line and end lines. This tells us how far we need to go.
                pageStart, pageEnd = lookup_line.split("\n")[0].split(",")
                pageStart = int(pageStart)
                pageEnd = int(pageEnd)


                # Now that we know how far we need to go in the data file, let's step
                # through the datafile, until we've reached the starting line of our page.
                while dataLineNumber < pageStart:
                    try:
                        # Try to read the next line, and increment the current line counter.
                        data_line = data_stream.readline()
                        dataLineNumber += 1
                    except:
                        # If we fail to read a line, throw an error.
                        error = "Failed to read new line from data_stream while seeking for new page!\nData File: {}\nLine Number: {}".format(data_file,dataLineNumber)
                        panic(error)

                # Great, we've finally reached the start of this page. Now, we can gather
                # the lines comprising this page and add them to the pageText variable.
                # print("Page: {}\t\tStart: {}\t\tEnd: {}".format(pageNumber, pageStart, pageEnd))
                while dataLineNumber <= pageEnd:
                    try:
                        # Try to read the next line, and increment the current line counter.
                        # Add the current line to the pageText variable for later parsing.
                        data_line = data_stream.readline()
                        pageText = pageText + data_line
                        dataLineNumber += 1
                    except:
                        # If we fail to read a line, throw an error.
                        error = "Failed to read new line from data_stream while adding data to known page.\nData File: {}\nLine Number: {}".format(data_file,dataLineNumber)


                # Now that we have a strong of text comprising the XML data we
                # wish to parse, it's time to do whatever we want to it. (process, cut, etc)
                process_page(pageText)
                if pageNumber > 12:
                    break

                # It's time to look at the next page, so let's increment our page counter.
                pageNumber += 1
                # print("\n")


def process_page(xml_string):
    page = ET.XML(xml_string)
    # print(xml_string)
    children = []
    p_title = ''
    p_id = ''
    p_ns = ''
    p_redirect = ''
    p_restrictions = ''
    for child in page.getchildren():
        if child.tag == "title":
            p_title = child.text
        elif child.tag == "id":
            p_id = child.text
        elif child.tag == "ns":
            p_ns = child.text
        elif child.tag == "revision":
            p_revision = child
        elif child.tag == "redirect":
            p_redirect = child.values()
            try:
                print(child.values()[1])
                error = "ERROR: When tagging this redirect object, we found more than 1 value. The program doesn't yet have a way to handle multiple values here. Please fix.\nXML Block: {}".format(xml_string)
                panic(error)
            except:
                # Nothing to see here. This exception is a good thing.
                pass
        elif child.tag == "restrictions":
            p_restrictions = child.text
        else:
            error = "ERROR: Unhandled Tag: {}".format(child.tag)
            panic(error
            )
    #     children.append((child, child.tag))
    # print(children)


    p_text = ''
    for child in p_revision:
        if child.tag == "text":
            p_text = child.text

    p_links = False
    if "http://" in p_text:
        p_links = True
    elif "https://" in p_text:
        p_links = True
    print(p_id, "\t", p_links, "\t", p_title)



print("ID", "\t", "LINKS", "\t", "TITLE")
print("=================================")

step_through_pages(data_file, lookup_file)
