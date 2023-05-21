from flask import Flask, jsonify, request
from neo4j import GraphDatabase

app = Flask(__name__)

def connect_to_neo4j():
    driver = GraphDatabase.driver("neo4j+s://fbb1df5b.databases.neo4j.io", auth=("neo4j", "NCJMoRmWFfrQtpkTivqCPLvhu0OiiIhd-9NVUgfUvRA"))
    return driver.session()

@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    try:
        session = connect_to_neo4j()
        result = session.run("MATCH (n) RETURN n")
        print(result)
        nodes = [dict(record['n']) for record in result]
        print(nodes)
        session.close()
        return jsonify(nodes), 200
    except:
        return jsonify({"Error:No se pudo ejecutar"}), 500


if __name__ == '__main__':
    app.run(debug=True)