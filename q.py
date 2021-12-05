import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.document import Document, Field, TextField
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

print('lucene', lucene.VERSION)

import sys

search_sets = 10

args = sys.argv[1:]

if args[0] == '-c':
    search_sets = int(args[1])
    args = args[2:]

query = ' '.join(args)
print(query)

directory = SimpleFSDirectory(Paths.get("tempIndex"))
searcher = IndexSearcher(DirectoryReader.open(directory))
analyzer = StandardAnalyzer()
query = QueryParser("set", analyzer).parse(query)
#query = QueryParser("set", analyzer).parse("search text")
scoreDocs = searcher.search(query, search_sets).scoreDocs
#print("%s sets matching." % len(scoreDocs))

final_set = []

for scoreDoc in scoreDocs:
    doc = searcher.doc(scoreDoc.doc)
    final_set.extend(doc.get("set").split('\t'))

print(list(set(final_set)))

