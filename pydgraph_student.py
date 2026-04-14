import csv
import json
import pydgraph

# ------------------------
# CONNECTION
# ------------------------
# Connection to Dgraph (localhost:9080)
client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)


# ------------------------
# SCHEMA
# ------------------------
def create_schema():

    schema = """
    type Product{
        name
        price
        category
    }
    type Supplier{
        supplier_name
        location
        supplies
    }
    type Customer{
        customer_name
        email
        city
    }
    type Order{
        order_id
        date
        total
        placed_by
        contains
    }

    name: string @index(term) .
    price: int .
    category: string @index(hash) .

    supplier_name: string .
    location: geo .
    supplies: [uid] @reverse .

    customer_name: string .
    email: string .
    city: string .

    order_id: string .
    date: string .
    total: int .
    placed_by: [uid] @reverse .
    contains: [uid] .
    """

    op = pydgraph.Operation(schema=schema)
    client.alter(op)


# ------------------------
# DROP ALL DATA
# ------------------------
def drop_all():
    confirm = input(
        "This will delete EVERYTHING in the graph. Continue? (y/n): ")

    if confirm.lower() == "y":
        op = pydgraph.Operation(drop_all=True)
        client.alter(op)
        print("Graph completely deleted")
    else:
        print("Operation cancelled")


# ------------------------
# GEO HELPER
# ------------------------
def parse_location(location_str):
    """
    Converts "lon,lat" → Geo JSON format

    Example:
    "-99.1332,19.4326" →
    {"type": "Point", "coordinates": [-99.1332, 19.4326]}
    """

    parts = location_str.split(",")

    lon = float(parts[0])
    lat = float(parts[1])

    return {
        "type": "Point",
        "coordinates": [lon, lat]
    }


# ------------------------
# LOAD PRODUCTS
# ------------------------
def load_products():
    txn = client.txn()
    try:
        products = []
        with open("products.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                products.append({
                    "uid": "_:" + row["name"],
                    "dgraph.type": "Product",
                    "name": row["name"],
                    "price": float(row["price"]),
                    "category": row["category"]
                })

        res = txn.mutate(set_obj=products)
        txn.commit()
        print("Products loaded successfully")
        return res.uids
    finally:
        txn.discard()


# ------------------------
# LOAD SUPPLIERS
# ------------------------
def load_suppliers():
    txn = client.txn()
    try:
        suppliers = []
        with open("suppliers.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                suppliers.append({
                    "uid": "_:" + row["supplier_name"],
                    "dgraph.type": "Supplier",
                    "supplier_name": row["supplier_name"],
                    "location": parse_location(row["location"])
                })

        res = txn.mutate(set_obj=suppliers)
        txn.commit()
        print("Suppliers loaded successfully")
        return res.uids
    finally:
        txn.discard()


# ------------------------
# CREATE RELATIONSHIPS
# ------------------------
def create_edges(product_uids, supplier_uids):
    txn = client.txn()
    try:
        with open("product_supplier.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                txn.mutate(set_obj={
                    "uid": supplier_uids[row["supplier_name"]],
                    "supplies": {
                        "uid": product_uids[row["product_name"]]
                    }
                })

        txn.commit()
        print("Relationships created successfully")
    finally:
        txn.discard()


# ------------------------
# RDF MUTATION (MANUAL INPUT)
# ------------------------
def run_rdf_mutation():
    print("\n--- RDF MUTATION ---")
    print("Paste your RDF mutation (double ENTER to execute):")

    lines = []
    while True:
        line = input()
        if line == "":
            break
        lines.append(line)

    rdf_mutation = "\n".join(lines)

    txn = client.txn()
    try:
        res = txn.mutate(set_nquads=rdf_mutation)
        txn.commit()
        print("Mutation executed successfully")
        print("Generated UIDs:", res.uids)

    except Exception as e:
        print("Mutation error:", e)

    finally:
        txn.discard()


# ------------------------
# QUERIES
# ------------------------

def query_products_by_name():
    name = input("Product name: ")
    query = """
    query q($name: string) {
        products(func: type(Product)) @filter(anyofterms(name, $name)) {
            name
            price
            category
        }
    }
    """
    res = client.txn(read_only=True).query(query, variables={'$name': name})
    print(json.dumps(json.loads(res.json), indent=2))


def query_products_by_price():
    price = input("Minimum product price: ")
    query = """
        query q($price: int) {
            products(func: type(Product)) @filter(ge(price, $price)) {
                name
                price
                category
                ~supplies{
                    supplier_name
                    location
                }
            }
        }
        """
    res = client.txn(read_only=True).query(query, variables={'$price': price})
    print(json.dumps(json.loads(res.json), indent=2))


def query_products_by_category():
    category = input("Category: ")
    query = """
        query q($category: string) {
            products(func: type(Product)) @filter(eq(category, $category)) {
                name
                price
                category
                ~supplies{
                    supplier_name
                    location
                }
            }
        }
        """
    res = client.txn(read_only=True).query(
        query, variables={'$category': category})
    print(json.dumps(json.loads(res.json), indent=2))


def query_customers_with_orders():
    city = input("City: ")
    query = """
        query q($city: string) {
            customer(func: type(Customer)) @filter(eq(city, $city)) {
                customer_name
                email
                city
                ~placed_by{
                    order_id
                    date
                    total
                    contains{
                        name
                        price
                        category
                    }
                }
            }
        }
        """
    res = client.txn(read_only=True).query(query, variables={'$city': city})
    print(json.dumps(json.loads(res.json), indent=2))


# ------------------------
# MENU
# ------------------------
def menu():
    while True:
        print("\n--- MENU ---")
        print("1. Create schema")
        print("2. Load CSV data")
        print("3. Run queries")
        print("4. Drop all data")
        print("5. Manual RDF mutation")
        print("6. Exit")

        option = input("Select an option: ")

        if option == "1":
            create_schema()

        elif option == "2":
            create_edges(load_products(), load_suppliers())

        elif option == "3":
            print("\n--- QUERIES ---")
            print("1. Products by name")
            print("2. Products by price")
            print("3. Products by category")
            print("4. Customers with orders")

            q = input("Select a query: ")

            if q == "1":
                query_products_by_name()
            elif q == "2":
                query_products_by_price()
            elif q == "3":
                query_products_by_category()
            elif q == "4":
                query_customers_with_orders()

        elif option == "4":
            drop_all()

        elif option == "5":
            run_rdf_mutation()

        elif option == "6":
            break

        else:
            print("Invalid option")


# ------------------------
# MAIN
# ------------------------
def main():
    menu()


if __name__ == "__main__":
    try:
        main()
    finally:
        client_stub.close()
