from flask import Flask, jsonify, request
from neo4j import GraphDatabase
import random
import string
import datetime
import random
from flask_cors import CORS


app = Flask(__name__)
cors = CORS(app)

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
    
@app.route('/api/sucursal_servicios', methods=['GET'])
def get_sucursal_servicios():
    try:
        nombre = request.headers.get('nombre')
        session = connect_to_neo4j()
        query = "MATCH (n:Sucursal)-[r:CONTRATA]->(s:Servicio) WHERE n.nombre = '%s' RETURN n, r, s"%(nombre)
        result = session.run(query)

        nodes = []
        for record in result:
            nodes.append({
                "servicio": dict(record['s']),
                "relacion": dict(record['r'])
            })

        session.close()
        return jsonify(nodes), 200
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500
    
@app.route('/api/trabajador_set_activo', methods=['POST'])
def get_trabajador_set_activo():
    try:
        content = request.json
        print(content)
        nombre = content['nombre']
        activo = content['activo']

        session = connect_to_neo4j()
        query = "MATCH (n:Persona:Trabajador)-[r:TRABAJA]->(s:Sucursal) WHERE n.nombre = '%s' SET r.activo = %s RETURN n"%(nombre, activo)
        result = session.run(query)

        session.close()
        return jsonify({"Message": "Trabajador actualizado exitosamente"}), 200
        
    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500
    
@app.route('/api/procto_presentacion', methods=['GET'])
def get_procto_presentacion():
    try:
        nombre = request.headers.get('nombre')
        session = connect_to_neo4j()
        query = "MATCH (n:Producto) WHERE n.nombre = '%s' RETURN n.presentaciones"%(nombre)
        result = session.run(query)

        pres = []
        for record in result:
            pres.append({
                "presentaciones": record['n.presentaciones']
            })

        session.close()
        return jsonify(pres), 200
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
        return jsonify({"Message": "No se pudo ejecutar"}), 500
    
@app.route('/api/notrabajadores', methods=['GET'])
def get_notrabajadores():
    try:
        session = connect_to_neo4j()
        result = session.run("MATCH (n:Persona) WHERE NOT 'Trabajador' IN LABELS(n) RETURN n")
        nodes = [dict(record['n']) for record in result]
        session.close()
        return jsonify(nodes), 200
    except:
        return jsonify({"Message": "No se pudo ejecutar"}), 500

@app.route('/api/trabajador_sucursal', methods=['GET'])
def get_trabajador_sucursal():
    try:
        nombre = request.headers.get('nombre')
        session = connect_to_neo4j()
        query = "MATCH (n:Sucursal)<-[r:TRABAJA]-(p:Persona:Trabajador) WHERE n.nombre = '%s' RETURN n, r, p"%(nombre)
        result = session.run(query)

        nodes = []
        for record in result:
            diccionario_relacion = dict(record['r'])
            try:
                diccionario_relacion['fecha_inicio'] = diccionario_relacion['fecha_inicio'].iso_format()
            except:
                pass
            nodes.append({
                "trabajador": dict(record['p']),
                "relacion": diccionario_relacion
            })

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
        result1 = session.run(query)

        nodes = []
        for record in result1:

            query = "MATCH (n:Pedido)-[r:CONTIENE]->(p:Producto) WHERE n.id = '%s' RETURN n, r, p"%(dict(record['p'])['id'])
            result2 = session.run(query)

            productos = []
            for record2 in result2:
                productos.append({
                    "producto": dict(record2['p']),
                    "relacion": dict(record2['r'])
                })

            
            
            query = "MATCH (n:Pedido)<-[r:ORDENO]-(p:Persona) WHERE n.id = '%s' RETURN n, r, p"%(dict(record['p'])['id'])
            result3 = session.run(query)

            persona = ''
            for record3 in result3:
                persona = dict(record3['p'])

            query = "MATCH (n:Pedido)-[r:ASOCIADO]->(p:Factura) WHERE n.id = '%s' RETURN n, r, p"%(dict(record['p'])['id'])
            result4 = session.run(query)

            factura = ''
            for record4 in result4:
                factura = dict(record4['p'])
            
            try:
                factura['fecha'] = factura['fecha'].iso_format()
            except:
                pass


            diccionario_pedido = dict(record['p'])

            try:
                diccionario_pedido['fecha_orden'] = diccionario_pedido['fecha_orden'].iso_format()
                diccionario_pedido['fecha_entrega'] = diccionario_pedido['fecha_entrega'].iso_format()
            except:
                pass
            

            nodes.append({
                "pedido": diccionario_pedido,
                "relacion": dict(record['r']),
                "productos": productos,
                "persona": persona,
                "factura": factura
            })

        session.close()
        return jsonify(nodes), 200

    except:
        return jsonify({"Error: No se pudo ejecutar"}), 500

def serialize_datetime(obj):

    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')  # Formato de fecha y hora deseado
    raise TypeError('Tipo de objeto no serializable')

@app.route('/api/sucursal_trabajador', methods=['POST'])
def create_sucursal_trabajador():
    try:
        content = request.json
        print(content)
        persona = content['persona']
        horario = content['horario']
        vehiculo = content['vehiculo']
        sueldo = float(content['sueldo'])
        puesto = content['puesto']
        area = content['area']
        fecha_inicio = content['fecha_inicio']
        contrato = content['contrato']
        activo = content['activo']
        sucursal = content['sucursal']
        
        
        session = connect_to_neo4j()

        
        query = "MATCH (p:Persona) WHERE p.nombre = '%s' SET p:Trabajador RETURN p"%(persona)
        result = session.run(query)

        query = "MATCH (p:Persona:Trabajador) WHERE p.nombre = '%s' SET p.horario = '%s', p.vehiculo = %s, p.sueldo = %f, p.puesto = '%s', p.area = '%s' RETURN p"%(persona, horario, vehiculo, sueldo, puesto, area)
        result = session.run(query)

        query = "MATCH (p:Persona:Trabajador), (s:Sucursal) WHERE p.nombre = '%s' AND s.nombre = '%s' CREATE (p)-[r:TRABAJA {contrato: %s, activo: %s, fecha_inicio: date('%s')}]->(s) RETURN p, r, s" % (persona, sucursal, contrato, activo, fecha_inicio)
        result = session.run(query)

        session.close()
        return jsonify({"Message": "Trabajador creado exitosamente"}), 200

    except Exception as e:
        print("El error es este")
        print(e)
        return jsonify({"Message": "Error, no se pudo ejecutar"}), 500

@app.route('/api/eliminar_trabajador', methods=['POST'])
def delete_trabajador():
    try:
        content = request.json
        print(content)
        persona = content['persona']
        sucursal = content['sucursal']


        session = connect_to_neo4j()

        query = "MATCH (p:Persona:Trabajador)-[r:TRABAJA]->(s:Sucursal) WHERE p.nombre = '%s' AND s.nombre = '%s' DELETE r RETURN p, r, s" % (persona, sucursal)
        result = session.run(query)


        # Eliminar horario, vehiculo, sueldo, puesto, area
        query = "MATCH (p:Persona:Trabajador) WHERE p.nombre = '%s' REMOVE p.horario, p.vehiculo, p.sueldo, p.puesto, p.area RETURN p"%(persona)

        query = "MATCH (p:Persona:Trabajador) WHERE p.nombre = '%s' REMOVE p:Trabajador RETURN p"%(persona)
        result = session.run(query)

        session.close()
        return jsonify({"Message": "Trabajador eliminado exitosamente"}), 200

    except Exception as e:
        print("El error es este")
        print(e)
        return jsonify({"Message": "Error, no se pudo ejecutar"}), 500

@app.route('/api/sucursal_pedidos', methods=['POST'])
def create_sucursal_pedidos():
    
    print(request.json)
    try:
        content = request.json
        persona = content['persona']
        direccion = content['direccion']
        productos = content['productos']
        presentaciones = content['presentaciones']
        descuento = content['descuento']
        cantidades = content['cantidades']
        tamaños = content['tamaños']

        mediocompra = content['mediocompra']

        if mediocompra == 'En linea':
            tipo = 'Electronico'
        else:
            tipo = 'Fisico'

        # fecha_orden = content['fecha_orden']
        # fecha_entrega = content['fecha_entrega']

        tiempo = random.uniform(10, 72)
        texto = str(tiempo) + ' hrs'

        fecha_orden = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).date()
        fecha_entrega = fecha_orden + datetime.timedelta(days=random.randint(3, 6))

        estado = 'En proceso'

        metodo_envio = content['metodo_envio']
        cancelado = content['cancelado']

        numero_pedido = get_num_pedido(content['sucursal'])
        servicio_mensajeria = content['servicio_mensajeria']
        tiempo_de_preparacion = content['tiempo_de_preparacion']

        metodo_pago = content['metodo_pago']

        contacto = content['contacto']
        moneda = content['moneda']

        transaccion = ''.join(random.choices(string.digits, k=10))

        serie = ''.join(random.choices(string.digits, k=4))

        while get_transacciones(transaccion):
            transaccion = random.choices(string.digits, k=10)
        
        transaccion = ''.join(transaccion)
        
        total = 0
        for i in range(len(productos)):
            precioU = get_precio(productos[i])
            total += cantidades[i] * precioU

        if descuento:   
            total -= total * 0.2
            
        iva = total * 0.12

        letras = random.choices(string.ascii_uppercase, k=3)
        numeros = random.choices(string.digits, k=3) 

        nuevo_id = ''.join(letras + numeros)

        while get_ids(nuevo_id):
            letras = random.choices(string.ascii_uppercase, k=3)
            numeros = random.choices(string.digits, k=3)

            nuevo_id = ''.join(letras + numeros)

        session = connect_to_neo4j()

        # crear el nuevo nodo de pedido
        #CUIDADO HAY QUE VER LA FECHA
        query = "CREATE (p:Pedido{id: '%s', fecha_orden: date('%s'), fecha_entrega: date('%s'), estado: '%s', metodo_envio: '%s', cancelado: %s}) RETURN p"%(nuevo_id, fecha_orden, fecha_entrega, estado, metodo_envio, cancelado)
        result = session.run(query)

        #crear el nuevo nodo de factura
        #CUIDADO HAY QUE VER LA FECHA
        query = "CREATE (f:Factura{fecha: date('%s'), total: %f, metodo_pago: '%s', numero_de_transaccion: %s, iva: %f, anulado: false}) RETURN f"%(fecha_orden, total, metodo_pago, transaccion, iva)
        result = session.run(query)
        
        # crear la relacion de pedido a sucursal
        query = "MATCH (n:Sucursal), (p:Pedido) WHERE n.nombre = '%s' AND p.id = '%s' CREATE (n)<-[r:SE_HACE{numero_pedido: %d, servicio_mensajeria: '%s', tiempo_de_preparacion: '%s'}]-(p) RETURN n, r, p"%(content['sucursal'], nuevo_id, numero_pedido, servicio_mensajeria, tiempo_de_preparacion)
        result = session.run(query)

        # crear la relacion de persona a pedido
        query = "MATCH (n:Persona), (p:Pedido) WHERE n.nombre = '%s' AND p.id = '%s' CREATE (n)-[r:ORDENO{direccion: '%s', medio_compra: '%s', descuento: %s}]->(p) RETURN n, r, p"%(persona, nuevo_id, direccion, mediocompra, descuento)
        result = session.run(query)

        #por cada producto se crea la realacion de pedido a producto
        for i in range(len(productos)):
            # crear la relacion de pedido a producto
            query = "MATCH (n:Pedido), (p:Producto) WHERE n.id = '%s' AND p.nombre = '%s' CREATE (n)-[r:CONTIENE{cantidad: %d, presentacion: '%s', tamano:'%s'}]->(p) RETURN n, r, p"%(nuevo_id, productos[i], cantidades[i], presentaciones[i], tamaños[i])
            result = session.run(query)
        
        # crear la relacion de factura a sucursal
        query = "MATCH (f:Factura), (s:Sucursal) WHERE f.numero_de_transaccion = %s AND s.nombre = '%s' CREATE (f)<-[r:EMITE{contacto: '%s', moneda:'%s', numero_de_serie:'%s'}]-(s) RETURN f, r, s"%(transaccion, content['sucursal'], contacto, moneda, serie)
        result = session.run(query)

        # crear la relacion de factura a pedido
        query = "MATCH (f:Factura), (p:Pedido) WHERE f.numero_de_transaccion = %s AND p.id = '%s' CREATE (f)<-[r:ASOCIADO{persona: '%s', descuento:%s, direccion:'%s'}]-(p) RETURN f, r, p"%(transaccion, nuevo_id, persona, descuento, direccion)
        result = session.run(query)
        
        # crear la relacion de factura a persona
        query = "MATCH (f:Factura), (p:Persona) WHERE f.numero_de_transaccion = %s AND p.nombre = '%s' CREATE (f)-[r:SE_ENTREGA{direccion: '%s', tipo: '%s', moneda:'%s'}]->(p) RETURN f, r, p"%(transaccion, persona, direccion, tipo, moneda)
        result = session.run(query)
        
        session.close()
        return jsonify({"Message": "Pedido creado exitosamente"}), 200

    except Exception as e:
        print("El error es este")
        print(e)
        return jsonify({"Message": "Error, no se pudo ejecutar"}), 500

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

        cantidad_pedidos = 0
        for record in result:
            cantidad_pedidos += 1

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

@app.route('/api/agregar_sucursal', methods=['POST'])
def create_sucursal():
    try:
        content = request.json
        session = connect_to_neo4j()

        query = "CREATE (n:Sucursal{nombre: '%s', direccion: '%s', nit: '%s', categoria: '%s', telefono: %d}) RETURN n"%(content['nombre'], content['direccion'], content['nit'], content['categoria'] , int(content['telefono']))
        result = session.run(query)
        session.close()
        return jsonify({"Message": "Sucursal creada exitosamente"}), 200
    except Exception as e:
        print(e)
        return jsonify({"Message": "No se pudo ejecutar"}), 500


@app.route('/api/eliminar_pedido', methods=['POST'])
def delete_pedido():
    try:
        content = request.json
        id_pedido = content['id_pedido']
        session = connect_to_neo4j()

        query = "MATCH (n:Pedido)-[r:ASOCIADO]->(s) WHERE n.id = '%s' RETURN s"%(id_pedido)
        result = session.run(query)
        print(result)
        
        num_transaccion = 0
        for record in result:
            num_transaccion = record['s']["numero_de_transaccion"]
        
        query = "MATCH (n:Factura) WHERE n.numero_de_transaccion = %d SET n.anulado = true RETURN n"%(num_transaccion)
        result = session.run(query)
        
        query = "MATCH (n:Pedido) WHERE n.id = '%s' DETACH DELETE n"%(id_pedido)
        result = session.run(query)
        
        session.close()
        return jsonify({"Message": "Pedido eliminado exitosamente"}), 200
    except Exception as e:
        
        print(e)
        return jsonify({"Message": "No se pudo ejecutar"}), 500

@app.route('/api/recogido', methods=['POST'])
def recogido():
    try:
        content = request.json
        id_pedido = content['id_pedido']
        session = connect_to_neo4j()

        query = "MATCH (n:Pedido)-[r:SE_HACE]->(s:Sucursal) WHERE n.id = '%s' REMOVE r.servicio_mensajeria RETURN n"%(id_pedido)
        result = session.run(query)

        session.close()
        return jsonify({"Message": "Pedido actualizado exitosamente"}), 200
    except:
        return jsonify({"Message": "No se pudo ejecutar"}), 500

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