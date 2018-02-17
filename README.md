# wiki_links


# ***Summary***
Goal: Create a neo4j graph/database, characterizing the relationships of wikipedia articles hyperlinking to eachother.

Written by: Daniel Bowder <br>
Started on: 2018-01-15 -- 2018 Jan 15 <br>
Github: https://github.com/DotBowder <br>

Version: 0.2 <br>

This program takes in the XML, enwiki wikimedia data dump for Wikipedia pages.

This program was designed to take the 64GB xml file, enwiki-20170820-pages-articles.xml,
find hyperlinks on each page, and determine what wikipedia pages link to other wikipedia pages.

The end goal of this program is to use neo4j to visualize, or just store, information
pertaining to the page to page hyperlinking relationships of wikipedia.

This can serve as a tool for discovering how different topics realate to eachother,
as characterized by wikipedia linking relationships.

This program uses lxml (partially) to parse the xml file, and extract useful information such as
website links, titles, and other information located on a wikipedia page. This information is then (currently) stored in a TSV file. The next step is to push this data to a neo4j database.

I'm happy to say the wasteful full pass of the data file is no longer necessary for generating
the original lookup table. The code now completes one full pass of the data file, and extracts
the Wikipedia ID, Wikipedia Page Title, and all WikiText Hyperlinks on the webpage from this single pass.

# ***Helpful links:*** <br>
Wikipedia "wikitext" parsing
https://en.wikipedia.org/wiki/Help:Wikitext#Links_and_URLs

Wikipedia Data Dump Torrents
https://meta.wikimedia.org/wiki/Data_dump_torrents

lxml syntax
http://lxml.de/parsing.html

# ***Next Goal:*** <br>

V0.4

- Add ability to define a relationship of a neo4j node.




# ***Change Log:*** <br>

V 0.2 -> 0.3

Complete re-write from the ground up.

- Adds functionality to create nodes in neo4j in a batch manor.
- Cleans up the code and add's a user cli front-end.

Created Functions
  - Data Stream Helpers
    - find_tag() * Seeks through text and searches for a given tag
    - get_next_page() * Uses find_tag to seek through text looking for "<page>", compiles whole <page></page> text into a block
  - XML Search Helpers
    - get_..._from_page_text() * Takes a block of text in, and returns whatever the function name implies "..."
    - get_..._from_page_parser() * Takes a closed lxml parser object, and returns whatever the function name implies "..."
  - Neo4J Helpers
    - connect * Returns graph from neo4j server after connecting
    - batch_create_nodes * Intakes a list of nodes in dictionary format. Queues nodes for transmission to neo4j server, and commits all nodes from dictionary in one transaction.
    - DELETE_ALL * Deletes all nodes and relationships from neo4j graph.
  - Disk Related Functions
    - save_... * Saves file to input "file_name". Various save functions for TSV lookup files & neo4j server profiles
    - load_... * Loads data from various files. Wiki Database Files, TSV lookup files, & neo4j server profiles
    - delete_file * Writes over a file passed to the function.

  - Main
    - Handles user input and directs the user to the correct place.

V 0.1 -> 0.2
step_through_pages 
- Now includes a lxml parser object and no longer requires trasversal of the whole dataset to generate a lookup table for the page numbers. The page nubmers COULD be useful, but, really isn't ideal for my purposes.
process_page 
- Collects Wikipedia ID, Wikipedia Title, and WikiText Links, and merges them into a list
 [0:WIKIPEDIA_ID,1:WIKIPEDIA_TITLE,2-INF:WIKI_LINKS]
find_wikitext_links 
- parses a string looking for WikiText links (charactorised by "[[" as an opener and "]]" as a closer) Returns list of links found in a given string
write_wikilinks_file 
- ingests masterlist (list of wikipedia ID, wikipedia Title, and wikipedia links located on article) and appents to a TSV formatted file.

V 0.0 -> 0.1
parse_page_lines 
- can step through a data file and generate a lookup table file, detailing the start and end lines containing <page> </page> xml tags.
step_through_pages 
- can iterate through the lookup table file, and use the start and end lines to walk back through the data file knowing right where to stop for each page. *inefficient? Yes, I'll impliment an lxml parser object and feed it the xml data later.
process_page 
- can look at the <page> xml object, and extract the wikipedia ID, Title, Text, and can determine if the Text of the page has a hyperlink.
