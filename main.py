# Written by: Daniel Bowder
# Started on: 2018-01-15 -- 2018 Jan 15
# Github: https://github.com/DotBowder
#
# Version: 0.2
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
#



# time for tracking process duration, sys for sys.exit()
import time, sys
# lxml for *some* data parsing
from lxml import etree as ET


data_file = 'data/enwiki-20170820-pages-articles.xml'


# Generic Exit Function.
def panic(data):
    print("\n\nERROR: The program has Quit.\nInfo: {}".format(data))
    sys.exit()


def step_through_pages(data_file):
    # Open data file, and look for "<page>" and ""</page>" tags.
    # Feed lines of <page> </page> into lxml parser object & pageText string.
    # Use lxml parser object and pageText string to extract Wiki ID, Wiki Title, & Wiki Links


    # Open Data File
    with open(data_file, 'r') as data_stream:

        # Variable to close the internal loop.
        close = False

        # We start on line 0 in the data file
        dataLineNumber = 0

        # We need a variable to track what page we're on. (The line number of the lookup file that we're on.)
        pageNumber = 0

        while close == False:

            # Create an lxml parser object to feed each data_line into. This
            # parser object is re-generated for each new page. The goal was to feed each line
            # to this parser object, but, it seems that this is unnecessary, as each wikipedia
            # page is trivial in size, we just need to know when we've finished
            # collecting one page in our string pageText variable.
            parser = ET.XMLParser(remove_blank_text=True) # may look into using XMLPullParser

            # pageText will contain a full string of this <page></page>.
            # pageText can be processed by an HTML/XML parser.
            pageText = ""

            # dataLine is a temporary variable to hold the current line of our data file.
            dataLine = ""

            # Read the first data_line from the data_stream
            try:
                data_line = data_stream.readline()
                dataLineNumber += 1
            except:
                error = "Failed to read first line from data_stream!\nData File: {}\nLine Number: 0".format(data_file)
                panic(error)

            # Check if our current data_line contianes <page>. if it is contained, continue, if <page>
            # isn't contained, continue reading each new data_line from the data_stream until we find <page>.
            while '<page>' not in data_line:
                try:
                    # Try to read the next line, and increment the current line counter.
                    # print("Waiting for xml tag <page>... Current dataLineNumber: {}".format(dataLineNumber))
                    data_line = data_stream.readline()
                    dataLineNumber += 1
                except:
                    # If we fail to read a line, throw an error.
                    error = "Failed to read new line from data_stream while seeking for new page!\nData File: {}\nLine Number: {}\nLast Sucessful Line: {}".format(data_file,dataLineNumber,data_line)
                    panic(error)


            # print("Found xml tag <page> on line {}".format(dataLineNumber))
            # We know where the <page> starts, feed this first line into the parser, and
            # add this line to the string variable pageText.
            pageStart = dataLineNumber
            pageText = pageText + data_line
            parser.feed(data_line)


            # Great, we've finally reached the start of this page. Now, we can gather
            # the lines comprising this page and add them to the lxml parser object.
            # Check and see if </page> is contained in this line. If it is not, continue
            # adding the current line to the parser and pageText objects. Stop when </page> is found.
            while '</page>' not in data_line:
                try:
                    # Try to read the next line, and increment the current line counter.
                    # Add the current line to the pageText variable for later parsing.
                    data_line = data_stream.readline()
                    pageText = pageText + data_line
                    parser.feed(data_line)
                    dataLineNumber += 1
                except:
                    # If we fail to read a line, throw an error.
                    error = "Failed to read new line from data_stream while adding data to known page.\nData File: {}\nLine Number: {}".format(data_file,dataLineNumber)


            # Now that we have a string of text comprising the XML data we
            # wish to parse, it's time to do whatever we want to it. (process, cut, etc)
            # Close our lxml parsing object, and finalize it into the page variable.
            parser = parser.close()
            process_page(parser, pageText)
            # print(pageText) # useful for troubleshooting/understanding what data we have currently.


            # The if statement below allows us to stop the code on a given line. This is useful for development purposes.
            # if pageNumber >= 100:
            #     error = "A page limit is in effect. To continue beyond page {}, modify the step_through_pages function."
            #     close = True

            # It's time to look at the next page, so let's increment our page counter.
            pageNumber += 1
            # print("\n")

def process_page(parser, pageText):

    # Use our find_wikitext_links function to parse pageText, looking for [[WIKILINK]] objects.
    # Returns a list of string objects which are Titles for various Wikipedia pages.
    wiki_links = find_wikitext_links(pageText)

    # Each <page> has several XML children. <title> and <id> are most notable here.
    #
    children = []
    p_title = ''
    p_id = ''
    p_ns = ''
    p_redirect = ''
    p_restrictions = ''
    for child in parser.getchildren():
        if child.tag == "title":
            p_title = child.text.replace(" ", "_")
        elif child.tag == "id":
            p_id = child.text
    #     children.append((child, child.tag))
    # print(children)

    # Print a table line showing the Wikipedia ID, the Link status, and Title of the Wikipedia Article.
    # print(p_id + "\t" + p_title + "\n\t\tLinks:", wiki_links, "\n")


    # Now that we've extracted our wiki_links, the <page> <title> and the <page> <id>, we're going to cram these objects
    # into a single list so that we can write it to a TSV file or a database.
    ######################################################################################################################
    ########## This will be replaced by a function that sends neo4j CREATE requests to a neo4js database/server ##########
    ######################################################################################################################
    # The order goes: [0:WIKIPEDIA_ID,1:WIKIPEDIA_TITLE,2-INF:WIKI_LINKS]
    masterlist = wiki_links
    masterlist.insert(0,p_title)
    masterlist.insert(0,p_id)
    # Convert to TSV format and write to wikilinks datafile.
    write_wikilinks_file("ramdisk/wikilinks.tsv", masterlist)


def find_wikitext_links(text):
    start = "[["
    end = "]]"
    wiki_links = []
    text = text.replace("\n", "")

    if start in text:
        chunks = text.split(start)
        for chunk in chunks:
            if end in chunk:
                if "|" in chunk:
                    chunk = chunk.split("|")[0]
                if "#" in chunk:
                    chunk = chunk.split("#")[0]
                wiki_links.append(chunk.split(end)[0].replace(" ", "_"))
    return wiki_links

def write_wikilinks_file(wikiLinksFile, masterlist):
    # Convert to TSV format and write to wikilinks datafile.
    print(masterlist[0])
    with open(wikiLinksFile,"a") as wikilinks_stream:
        line = ''
        for item in masterlist:
            line = line + item + "\t"
        line = line + "\n"
        wikilinks_stream.write(line)

step_through_pages(data_file)
