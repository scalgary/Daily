#!/usr/bin/env python3
"""
create_big_file.py - Crée un fichier CSV qui va poser problème
"""

import csv
import random
import string
from datetime import datetime, timedelta


def create_massive_csv(filename, num_rows):
    """Créer un CSV avec beaucoup de colonnes et de données"""
    print(f"Création de {filename} avec {num_rows:,} lignes...")
    
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        
        # Header avec beaucoup de colonnes
        headers = [
            'id', 'user_id', 'transaction_id', 'product_name', 
            'category', 'subcategory', 'description', 'price', 
            'quantity', 'total', 'discount', 'tax', 'shipping',
            'customer_name', 'email', 'phone', 'address', 'city',
            'country', 'postal_code', 'payment_method', 'status',
            'created_at', 'updated_at', 'notes', 'tags'
        ]
        writer.writerow(headers)
        
        # Générer les données
        for i in range(num_rows):
            if i % 100000 == 0:
                print(f"  {i:,} lignes créées...")
            
            row = [
                i,  # id
                random.randint(1, 100000),  # user_id
                ''.join(random.choices(string.ascii_uppercase + string.digits, k=20)),  # transaction_id
                f"Product_{random.randint(1, 10000)}",  # product_name
                f"Category_{random.randint(1, 100)}",  # category
                f"SubCat_{random.randint(1, 500)}",  # subcategory
                ''.join(random.choices(string.ascii_letters, k=100)),  # description longue
                round(random.uniform(10, 1000), 2),  # price
                random.randint(1, 10),  # quantity
                round(random.uniform(10, 10000), 2),  # total
                round(random.uniform(0, 100), 2),  # discount
                round(random.uniform(0, 100), 2),  # tax
                round(random.uniform(5, 50), 2),  # shipping
                f"Customer_{random.randint(1, 100000)}",  # customer_name
                f"email{random.randint(1, 100000)}@example.com",  # email
                f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",  # phone
                ''.join(random.choices(string.ascii_letters, k=50)),  # address
                f"City_{random.randint(1, 1000)}",  # city
                random.choice(['USA', 'Canada', 'UK', 'France', 'Germany', 'Japan']),  # country
                f"{random.randint(10000, 99999)}",  # postal_code
                random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash']),  # payment_method
                random.choice(['Pending', 'Completed', 'Cancelled', 'Refunded']),  # status
                (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),  # created_at
                datetime.now().isoformat(),  # updated_at
                ''.join(random.choices(string.ascii_letters, k=200)),  # notes longues
                ','.join([f"tag_{random.randint(1, 100)}" for _ in range(10)])  # tags multiples
            ]
            writer.writerow(row)
    
    print(f"✓ Fichier créé: {filename}")

# Créer 3 fichiers de tailles différentes
if __name__ == "__main__":
    # Fichier petit pour tester (100K lignes ~ 100MB)
    create_massive_csv("data_small.csv", 100000)
    
    # Fichier moyen (1M lignes ~ 1GB)
    create_massive_csv("data_medium.csv", 1000000)
    
    # Fichier gros (5M lignes ~ 5GB) - Va poser problème!
    create_massive_csv("data_large.csv", 5000000)