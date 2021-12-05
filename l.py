import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, Field, TextField
from org.apache.lucene.store import SimpleFSDirectory

lucene.initVM(vmargs=['-Djava.awt.headless=true'])

print('lucene', lucene.VERSION)

store = SimpleFSDirectory(Paths.get("tempIndex"))
analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)
config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
writer = IndexWriter(store, config)

import os

def abs_paths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))

for path in abs_paths("/sets.csv"):
    print(path)
 
    file = open(path)
    contents = file.read()
    #contents = contents[:50]
    file.close()

    for line in contents.splitlines():
        doc = Document()
        doc.add(Field("set", line, TextField.TYPE_STORED))
        writer.addDocument(doc)

#doc = Document()
#text = "This is the text to be indexed."
#doc.add(Field("fieldname", text, TextField.TYPE_STORED))

writer.commit()
writer.close()

