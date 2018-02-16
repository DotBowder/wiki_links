# wiki_links
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

***Helpful links:*** <br>
Wikipedia "wikitext" parsing
https://en.wikipedia.org/wiki/Help:Wikitext#Links_and_URLs

Wikipedia Data Dump Torrents
https://meta.wikimedia.org/wiki/Data_dump_torrents

lxml syntax
http://lxml.de/parsing.html

***Next Goal:*** <br>
Add a new function to send "CREATE" requests to a neo4j database for understanding the relationship of Wikipedia articles linking to other Wikipedia articles.

