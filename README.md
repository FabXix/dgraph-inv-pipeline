# Graph Inventory System (Dgraph + Python)

## Overview
Graph-based inventory system using Dgraph and Python to manage products, suppliers, customers, and orders.

## Data Model
- Product
- Supplier
- Customer
- Order

Relationships:
- Suppliers supply Products
- Customers place Orders
- Orders contain Products

## Tech Stack
- Python 3
- Dgraph
- pydgraph
- CSV-based ETL

## Project Structure
.
├── main.py

├── products.csv

├── suppliers.csv

├── product_supplier.csv

## Features
- Schema creation
- CSV data loading (ETL)
- Relationship creation
- GraphQL queries
- RDF manual mutation support
- Geospatial parsing

## How to Run

Start Dgraph:
localhost:9080

Install dependencies:
pip install pydgraph

Run:
python main.py

## Example Query
query {
  products(func: type(Product)) @filter(eq(category, "Electronics")) {
    name
    price
  }
}
