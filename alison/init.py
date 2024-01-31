import sqlite3
import os

import pandas as pd

import whoosh as wh
from whoosh import fields
from whoosh import index
from whoosh import qparser

THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))

con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/data.db"))
df_lib = pd.read_sql_query("SELECT * FROM libros", con)
con.close()

df_lib[["id_libro","titulo","autor","editorial","genero"]] = df_lib[["id_libro","titulo","autor","editorial","genero"]].fillna(" ")

# TODO: ver field_boost en wh.fields
schema = wh.fields.Schema(
    id_libro=wh.fields.ID(stored=True),
    autor=wh.fields.ID(),
    editorial=wh.fields.ID(),
    genero=wh.fields.ID()
)

ix = wh.index.create_in("indexdir", schema)

writer = ix.writer()
for index, row in df_lib.iterrows():
    writer.add_document(id_libro=row["id_libro"],
                        autor=row["autor"],
                        editorial=row["editorial"],
                        genero=row["genero"]
    )
writer.commit()


terminos = [wh.query.Term("editorial", "DEBOLSILLO"), wh.query.Term("editorial", "PLANETA")]
query = wh.query.Or(terminos)

with ix.searcher() as searcher:
    results = searcher.search(query, terms=True)
    for r in results:
        print(r)
