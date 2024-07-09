import certifi
from pymongo import MongoClient
import os


def drop_collections(uri, database_name):
    # Conectar ao MongoDB
    client = MongoClient(uri, tlsCAFile=certifi.where())
    db = client[database_name]

    # Obter todas as coleções do banco de dados
    collections = db.list_collection_names()

    # Filtrar coleções que começam com "sandbox." e contêm "hybridflow"
    collections_to_drop = [col for col in collections if col.startswith("todrop.")]

    # Imprimir coleções a serem dropadas
    print("Coleções a serem dropadas:")
    for col in collections_to_drop:
        print(col)

    # Realizar o drop das coleções
    for col in collections_to_drop:
        db[col].drop()
        print(f'{col} dropped')


if __name__ == "__main__":
    # Parâmetros de conexão
    uri = os.environ['MONGO_URI']
    database_name = os.environ["DB_NAME"]

    # Chamar a função
    drop_collections(uri, database_name)
