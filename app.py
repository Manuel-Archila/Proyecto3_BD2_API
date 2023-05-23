from flask import Flask, jsonify, request
from neo4j import GraphDatabase
import random
import string

app = Flask(__name__)

def connect_to_neo4j():
    driver = GraphDatabase.driver("neo4j+s://fbb1df5b.databases.neo4j.io", auth=("neo4j", "NCJMoRmWFfrQtpkTivqCPLvhu0OiiIhd-9NVUgfUvRA"))
    return driver.session()

@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    try:
        session = connect_to_neo4j()
        result = session.run("MATCH (n) RETURN n")
        #print(result)
        nodes = [dict(record['n']) for record in result]
        #print(nodes)
        session.close()
        return jsonify(nodes), 200
    except:
        return jsonify({"Error:No se pudo ejecutar"}), 500

@app.route('/api/sucursales', methods=['GET'])
def get_sucursales():
    try:
        session = connect_to_neo4j()
        result = session.run("MATCH (n:Sucursal) RETURN n")
        #print(result)
        nodes = [dict(record['n']) for record in result]
        #print(nodes)
        session.close()
        return jsonify(nodes), 200
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500
    
@app.route('/api/sucursal_productos', methods=['GET'])
def get_sucursal_productos():
    try:
        nombre = request.headers.get('nombre')
        #nombre = "La Torre"
        session = connect_to_neo4j()
        query = "MATCH (n:Sucursal)-[r:TIENE]->(p:Producto) WHERE n.nombre = '%s' RETURN n, r, p"%(nombre)
        result = session.run(query)

        nodes = []
        for record in result:
            nodes.append({
                "producto": dict(record['p']),
                "relacion": dict(record['r'])
            })

        session.close()
        return jsonify(nodes), 200
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

@app.route('/api/personas', methods=['GET'])
def get_personas():
    try:
        session = connect_to_neo4j()
        result = session.run("MATCH (n:Persona) RETURN n")
        nodes = [dict(record['n']) for record in result]
        session.close()
        return jsonify(nodes), 200
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

@app.route('/api/sucursal_pedidos', methods=['GET'])
def get_sucursal_pedidos():
    try:
        nombre = request.headers.get('nombre')

        session = connect_to_neo4j()
        query = "MATCH (n:Sucursal)<-[r:SE_HACE]-(p:Pedido) WHERE n.nombre = '%s' RETURN n, r, p"%(nombre)
        result = session.run(query)

        nodes = []
        for record in result:
            nodes.append({
                "pedido": dict(record['p']),
                "relacion": dict(record['r'])
            })

        session.close()
        return jsonify(nodes), 200

    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

@app.route('/api/sucursal_pedidos', methods=['POST'])
def create_sucursal_pedidos():
    try:
        content = request.json
        persona = content['persona']
        direccion = content['direccion']
        productos = content['productos']
        presentaciones = content['presentaciones']
        descuento = content['descuento']
        cantidades = content['cantidades']
        tamaños = content['tamaños']
        calificaciones = content['calificaciones']
        comentarios = content['comentarios']

        fecha_orden = content['fecha_orden']
        fecha_entrega = content['fecha_entrega']
        estado = content['estado']
        metodo_envio = content['metodo_envio']
        cancelado = content['cancelado']

        numero_pedido = get_num_pedido(content['sucursal'])
        servicio_mensajeria = content['servicio_mensajeria']
        tiempo_de_preparacion = content['tiempo_de_preparacion']

        metodo_pago = content['metodo_pago']

        contacto = content['contacto']
        moneda = content['moneda']

        transaccion = random.choises(string.digits, k=10)

        serie = random.choises(string.digits, k=4)

        while get_transacciones(transaccion):
            transaccion = random.choises(string.digits, k=10)
        
        total = 0
        for i in range(len(productos)):
            precioU = get_precio(productos[i])
            total += cantidades[i] * precioU

        iva = total * 0.12

        letras = random.choises(string.ascii_uppercase, k=3)
        numeros = random.choises(string.digits, k=3) 

        nuevo_id = ''.join(letras + numeros)

        while get_ids(nuevo_id):
            letras = random.choises(string.ascii_uppercase, k=3)
            numeros = random.choises(string.digits, k=3)

            nuevo_id = ''.join(letras + numeros)

        session = connect_to_neo4j()

        # crear el nuevo nodo de pedido
        #CUIDADO HAY QUE VER LA FECHA
        query = "CREATE (p:Pedido{id: '%s', fecha_orden: '%s', fecha_entrega: '%s', estado: '%s', metodo_envio: '%s', cancelado: %s}) RETURN p"%(nuevo_id, fecha_orden, fecha_entrega, estado, metodo_envio, cancelado)
        result = session.run(query)

        #crear el nuevo nodo de factura
        #CUIDADO HAY QUE VER LA FECHA
        query = "CREATE (f:Factura{fecha: '%s', total: %f, metodo_pago: '%s', numero_de_transaccion: %d, iva: %f}) RETURN f"%(fecha_orden, total, metodo_pago, transaccion, iva)

        # crear la relacion de pedido a sucursal
        query = "MATCH (n:Sucursal), (p:Pedido) WHERE n.nombre = '%s' AND p.id = '%s' CREATE (n)<-[r:SE_HACE{numero_pedido: %d, servicio_mensajeria: '%s', tiempo_de_preparacion: '%s'}]-(p) RETURN n, r, p"%(content['sucursal'], nuevo_id, numero_pedido, servicio_mensajeria, tiempo_de_preparacion)
        result = session.run(query)

        # crear la relacion de persona a pedido
        query = "MATCH (n:Persona), (p:Pedido) WHERE n.nombre = '%s' AND p.id = '%s' CREATE (n)-[r:ORDENO{direccion: '%s', productos: %s, presentaciones: %s, descuento: %s}]->(p) RETURN n, r, p"%(persona, nuevo_id, direccion, productos, presentaciones, descuento)
        result = session.run(query)

        #por cada producto se crea la realacion de pedido a producto
        for i in range(len(productos)):
            # crear la relacion de pedido a producto
            query = "MATCH (n:Pedido), (p:Producto) WHERE n.id = '%s' AND p.nombre = '%s' CREATE (n)-[r:CONTIENE{cantidad: %d, presentacion: '%s', tamano:'%s'}]->(p) RETURN n, r, p"%(nuevo_id, productos[i], cantidades[i], presentaciones[i], tamaños[i])
            result = session.run(query)
            
            # crear la realacion de factura a producto
            query = "MATCH (n:Factura), (p:Producto) WHERE n.numero_de_transaccion = '%s' AND p.nombre = '%s' CREATE (n)-[r:TIENE{cantidad: %d, presentacion: '%s', numero_serie:'%s'}]->(p) RETURN n, r, p"%(transaccion, productos[i], cantidades[i], presentaciones[i], serie)
            result = session.run(query)

        
        # crear la relacion de factura a sucursal
        query = "MATCH (f:Factura), (s:Sucursal) WHERE f.numero_de_transaccion = '%s' AND s.nombre = '%s' CREATE (f)<-[r:EMITE{contacto: %s, moneda:'%s', numero_de_serie:'%s']-(s) RETURN n, r, p"%(transaccion, content['sucursal'], contacto, moneda, serie)
        result = session.run(query)

        # crear la relacion de factura a pedido
        query = "MATCH (f:Factura), (p:Pedido) WHERE f.numero_de_transaccion = '%s' AND p.id = '%s' CREATE (f)<-[r:ASOCIADO{persona: '%s', descuento:%s, direccion:'%s']-(p) RETURN n, r, p"%(transaccion, nuevo_id, persona, descuento, direccion)
        result = session.run(query)
        
        # crear la relacion de factura a persona
        query = "MATCH (f:Factura), (p:Persona) WHERE f.numero_de_transaccion = '%s' AND p.nombre = '%s' CREATE (f)-[r:SE_ENTREGA{direccion: '%s', descuento:%s, moneda:'%s']->(p) RETURN n, r, p"%(transaccion, persona, direccion, descuento, moneda)
        result = session.run(query)
        
        session.close()
        return jsonify({"Success": "Pedido creado exitosamente"}), 200

    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

def get_ids(nuevo_id):
    try:
        session = connect_to_neo4j()
        result = session.run("MATCH (p:Pedido) RETURN p.id")

        ids = []
        for record in result:
            ids.append(record['p.id'])

        # nodes = [dict(record['p']) for record in result]
        session.close()
        return nuevo_id in ids
    
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

def get_transacciones(nueva_transaccion):
    try:
        session = connect_to_neo4j()
        result = session.run("MATCH (f:Factura) RETURN f.numero_de_transaccion")

        transacciones = []
        for record in result:
            transacciones.append(record['f.numero_de_transaccion'])

        session.close()
        return nueva_transaccion in transacciones
    
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

def get_num_pedido(sucursal):
    try:
        
        session = connect_to_neo4j()
        query = "MATCH (n:Sucursal)<-[r:SE_HACE]-(p:Pedido) WHERE n.nombre = '%s' RETURN n, r, p"%(sucursal)
        result = session.run(query)

        cantidad_pedidos = len(result)

        session.close()
        return cantidad_pedidos + 1
    
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500


def get_precio(producto):
    try:
        session = connect_to_neo4j()
        query = "MATCH (n:Producto) WHERE n.nombre = '%s' RETURN n.precio_unitario"%(producto)
        result = session.run(query)
        
        for record in result:
            precio = record['n.precio_unitario']
       
        return precio
    
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

@app.route('/api/prueba', methods=['GET'])
def get_prueba():
    try:
        precio = get_precio('Limon')
        print(precio)
        return jsonify(precio), 200
        
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500


if __name__ == '__main__':
    app.run(debug=True)