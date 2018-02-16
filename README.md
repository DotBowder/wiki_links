# wiki_links
Goal: Create a neo4j graph/database, characterizing the relationships of wikipedia articles hyperlinking to eachother.

Written by: Daniel Bowder <br>
Started on: 2018-01-15 -- 2018 Jan 15 <br>
Github: https://github.com/DotBowder <br>


This program takes in the XML, enwiki wikimedia data dump for Wikipedia pages. <br>
Located Here: https://meta.wikimedia.org/wiki/Data_dump_torrents

This program was designed to take the 64GB xml file, enwiki-20170820-pages-articles.xml,
find hyperlinks on each page, and determine what wikipedia pages link to other wikipedia pages.

The end goal of this program is to use neo4j to visualize, or just store, information
pertaining to the page to page hyperlinking relationships of wikipedia.

This can serve as a tool for discovering how different topics realate to eachother,
as characterized by wikipedia linking relationships.


This program uses lxml to parse the xml file, and extract useful information such as
website links, titles, and other information located on a wikipedia page.

This program, currently, makes a full pass of the data file, to generate a lookup file
referencing the start and end lines of each XML <page> tag. This effectivly chops the
ingress data file into managable page chunks. The full pass isn't necessary if a lxml
prase object was fed each line, but, this is what made sense to me initially, before
exploring lxml deeper. This lxml parser may be implimented at a later date. <br>
See: http://lxml.de/parsing.html
