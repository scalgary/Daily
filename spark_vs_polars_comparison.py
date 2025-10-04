

import time
import polars as pl
import psutil
import multiprocessing
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import functions as F
import pyarrow 

def monitor_memory():
    """Affiche l'utilisation mémoire"""
    mem = psutil.virtual_memory()
    return f"RAM: {mem.percent}% ({mem.used/1024**3:.1f}GB utilisés)"

# ============================================
# CONFIGURATION DES RESSOURCES
# ============================================
num_cores = multiprocessing.cpu_count()
total_memory_gb = psutil.virtual_memory().total / (1024**3)
spark_memory = int(total_memory_gb * 1)  # 70% de la RAM totale

print("="*60)
print("CONFIGURATION DES RESSOURCES")
print("="*60)
print(f"Cœurs disponibles: {num_cores}")
print(f"RAM totale: {total_memory_gb:.1f} GB")
print(f"RAM pour Spark: {spark_memory} GB")
print(f"{monitor_memory()}")

# ============================================
# 1. TEST AVEC POLARS
# ============================================
print("\n" + "="*60)
print("TEST POLARS - Lecture optimisée")
print("="*60)

start_total_polars = time.time()

# 1. LECTURE LAZY
print("\n► Lecture LAZY du fichier...")
start = time.time()
df_polars = pl.scan_csv("data_medium.csv")
print(f" ✓ Scan lazy: {time.time()-start:.3f}s")
print(f" {monitor_memory()}")

# Afficher le schéma (CORRECTION ICI)
print("\n► Schéma des données:")
schema = df_polars.collect_schema()
column_names = schema.names()  # Utiliser .names() au lieu de .columns
print(f" Colonnes: {column_names}")
print(f" Types: {schema}")

# 2. OPÉRATION DE TEST (exemple: groupby + aggregation)
print("\n► Exécution d'une requête (groupby + count)...")
start = time.time()
result_polars = (
    df_polars
    .group_by(column_names[0])
    .agg(pl.len())
    .sort(column_names[0])  # ← Ajout du tri
    .collect()
)
time_query_polars = time.time() - start
print(result_polars.head(10))  # 10 premières lignes
print(f" ✓ Requête exécutée: {time_query_polars:.3f}s")
print(f" {monitor_memory()}")
print(f" Résultat: {len(result_polars)} groupes")

time_total_polars = time.time() - start_total_polars
print(f"\n⏱️  TEMPS TOTAL POLARS: {time_total_polars:.3f}s")

# ============================================
# 2. TEST AVEC SPARK
# ============================================
print("\n" + "="*60)
print("TEST SPARK - Configuration maximale")
print("="*60)

start_total_spark = time.time()

# Configuration Spark optimisée
print("\n► Initialisation de Spark...")
start = time.time()
spark = SparkSession.builder \
    .appName("SparkBenchmark") \
    .master(f"local[{num_cores}]") \
    .config("spark.driver.memory", f"{spark_memory}g") \
    .config("spark.executor.memory", f"{spark_memory}g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.default.parallelism", num_cores * 2) \
    .config("spark.sql.shuffle.partitions", num_cores * 2) \
    .getOrCreate()
print(f" ✓ Spark initialisé: {time.time()-start:.3f}s")
print(f" {monitor_memory()}")

# 1. LECTURE LAZY
print("\n► Lecture du fichier (lazy en Spark aussi)...")
start = time.time()
df_spark = spark.read.option("header", "true").option("inferSchema", "true").csv("data_medium.csv")
print(f" ✓ Définition du DataFrame: {time.time()-start:.3f}s")
print(f" {monitor_memory()}")

# Afficher le schéma
print("\n► Schéma des données:")
print(f" Colonnes: {df_spark.columns}")
df_spark.printSchema()

# 2. OPÉRATION DE TEST (même opération que Polars)
print("\n► Exécution d'une requête (groupby + count)...")
start = time.time()
#result_spark = df_spark.groupBy(df_spark.columns[0]).count().collect()
result_spark = (
    df_spark
    .groupBy(df_spark.columns[0])
    .agg(F.count("*").alias("len"))
    .orderBy(df_spark.columns[0])  # ← Ajout du tri
    .collect()
)
time_query_spark = time.time() - start
print(result_spark[:10])  # Affiche les 10 premières lignes

print(f" ✓ Requête exécutée: {time_query_spark:.3f}s")
print(f" {monitor_memory()}")
print(f" Résultat: {len(result_spark)} groupes")

time_total_spark = time.time() - start_total_spark
print(f"\n⏱️  TEMPS TOTAL SPARK: {time_total_spark:.3f}s")

# ============================================
# 3. COMPARAISON FINALE
# ============================================
print("\n" + "="*60)
print("RÉSULTATS DU BENCHMARK")
print("="*60)
print(f"\nPolars:")
print(f"  - Temps de requête: {time_query_polars:.3f}s")
print(f"  - Temps total: {time_total_polars:.3f}s")
print(f"\nSpark:")
print(f"  - Temps de requête: {time_query_spark:.3f}s")
print(f"  - Temps total: {time_total_spark:.3f}s")

# Calcul du speedup
speedup = time_query_spark / time_query_polars
print(f"\n🏆 Polars est {speedup:.2f}x {'plus rapide' if speedup > 1 else 'plus lent'} que Spark")
print(f"   (pour cette requête spécifique)")
result_spark_df = spark.createDataFrame(result_spark)
# Afficher les résultats côte à côte
print("\n► POLARS (10 premières lignes):")
print(result_polars.head(10))

print("\n► SPARK (10 premières lignes):")
result_spark_df.show(10)

# Vérification : nombre de groupes
print("\n► Vérification basique:")
print(f"  Nombre de groupes Polars: {len(result_polars)}")
print(f"  Nombre de groupes Spark: {len(result_spark)}")

if len(result_polars) == len(result_spark):
    print("  ✓ Même nombre de groupes")
else:
    print("  ✗ ATTENTION: Nombre de groupes différent!")

# Comparaison détaillée (conversion en Pandas pour faciliter)
print("\n► Comparaison détaillée des valeurs...")
result_polars_pd = result_polars.sort(column_names[0]).to_pandas()
result_spark_pd = result_spark_df.toPandas().sort_values(by=result_spark_df.columns[0]).reset_index(drop=True)

# Renommer les colonnes de Spark pour matcher Polars si nécessaire
result_spark_pd.columns = result_polars_pd.columns

# Comparer les DataFrames
if result_polars_pd.equals(result_spark_pd):
    print("  ✓ Les résultats sont IDENTIQUES")
else:
    print("  ⚠ Les résultats diffèrent légèrement")
    
    # Trouver les différences
    comparison = result_polars_pd.compare(result_spark_pd)
    if not comparison.empty:
        print("\n  Différences trouvées:")
        print(comparison.head(20))
    
    # Vérifier les sommes totales
    sum_polars = result_polars_pd['len'].sum()
    sum_spark = result_spark_pd['len'].sum()
    print(f"\n  Somme totale Polars: {sum_polars}")
    print(f"  Somme totale Spark: {sum_spark}")
    
    if sum_polars == sum_spark:
        print("  ✓ Les sommes totales correspondent (différences probablement dues au tri)")
# ============================================
# 4. COMPARAISON DÉTAILLÉE DES RÉSULTATS
# ============================================
print("\n" + "="*60)
print("COMPARAISON DÉTAILLÉE DES RÉSULTATS")
print("="*60)

# Convertir Spark en DataFrame
result_spark_df = spark.createDataFrame(result_spark)

# 1. APERÇU VISUEL CÔTE À CÔTE
print("\n► APERÇU DES RÉSULTATS:")
print("\nPOLARS (10 premières lignes):")
print(result_polars.head(10))

print("\nSPARK (10 premières lignes):")
result_spark_df.show(10, truncate=False)

# 2. STATISTIQUES DE BASE
print("\n► STATISTIQUES:")
print(f"Nombre de groupes Polars: {len(result_polars)}")
print(f"Nombre de groupes Spark:  {len(result_spark)}")

sum_polars = result_polars.select(pl.col("len").sum()).item()
sum_spark = sum(row['len'] for row in result_spark)
print(f"\nSomme totale des counts:")
print(f"  Polars: {sum_polars:,}")
print(f"  Spark:  {sum_spark:,}")
print(f"  Différence: {abs(sum_polars - sum_spark):,}")

# 3. CONVERSION EN PANDAS POUR COMPARAISON DÉTAILLÉE
print("\n► COMPARAISON DÉTAILLÉE (via Pandas)...")
result_polars_pd = result_polars.to_pandas().sort_values(by=column_names[0]).reset_index(drop=True)
result_spark_pd = result_spark_df.toPandas().sort_values(by=result_spark_df.columns[0]).reset_index(drop=True)
# Trier les deux DataFrames de la même manière
result_polars_pd = (
    result_polars
    .to_pandas()
    .sort_values(by=column_names[0])
    .reset_index(drop=True)
)

result_spark_pd = (
    result_spark_df
    .toPandas()
    .sort_values(by=result_spark_df.columns[0])
    .reset_index(drop=True)
)
# S'assurer que les colonnes ont les mêmes noms
result_spark_pd.columns = result_polars_pd.columns

# VÉRIFICATION SUPPLÉMENTAIRE : S'assurer que les types sont identiques
print(f"\nTypes Polars: {result_polars_pd.dtypes.to_dict()}")
print(f"Types Spark:  {result_spark_pd.dtypes.to_dict()}")

# Convertir les types si nécessaire pour une comparaison stricte
# (parfois Spark peut retourner int64 alors que Polars retourne int32, ou vice versa)
for col in result_polars_pd.columns:
    if col == 'len':
        # S'assurer que les counts sont du même type
        result_polars_pd[col] = result_polars_pd[col].astype('int64')
        result_spark_pd[col] = result_spark_pd[col].astype('int64')
    else:
        # Pour la colonne de groupement, convertir en string si nécessaire
        if result_polars_pd[col].dtype != result_spark_pd[col].dtype:
            result_polars_pd[col] = result_polars_pd[col].astype(str)
            result_spark_pd[col] = result_spark_pd[col].astype(str)

# 4. VÉRIFICATION D'ÉGALITÉ STRICTE
print("\n► TEST D'ÉGALITÉ STRICTE:")

# Test 1: Égalité complète
if result_polars_pd.equals(result_spark_pd):
    print("✓ Les résultats sont STRICTEMENT IDENTIQUES !")
    print("  - Même ordre")
    print("  - Mêmes valeurs")
    print("  - Mêmes types")
else:
    print("⚠ Les résultats présentent des différences\n")
    
    # Test 2: Égalité des valeurs (ignorer les types)
    try:
        # Comparer valeur par valeur
        values_equal = (result_polars_pd.values == result_spark_pd.values).all()
        if values_equal:
            print("✓ Les VALEURS sont identiques (différence de types seulement)")
        else:
            print("✗ Les VALEURS diffèrent")
    except:
        print("✗ Impossible de comparer les valeurs directement")
    
    # Test 3: Vérification élément par élément
    print("\n► DÉTAILS DES DIFFÉRENCES:")
    
    # Comparer les shapes
    if result_polars_pd.shape != result_spark_pd.shape:
        print(f"✗ Shapes différentes:")
        print(f"  Polars: {result_polars_pd.shape}")
        print(f"  Spark:  {result_spark_pd.shape}")
    else:
        print(f"✓ Même shape: {result_polars_pd.shape}")
    
    # Comparer ligne par ligne
    differences_found = False
    for idx in range(min(len(result_polars_pd), len(result_spark_pd))):
        polars_row = result_polars_pd.iloc[idx]
        spark_row = result_spark_pd.iloc[idx]
        
        if not polars_row.equals(spark_row):
            if not differences_found:
                print("\n✗ Premières différences trouvées:")
                differences_found = True
            
            if idx < 5:  # Afficher seulement les 5 premières différences
                print(f"\n  Ligne {idx}:")
                print(f"    Polars: {polars_row.to_dict()}")
                print(f"    Spark:  {spark_row.to_dict()}")
    
    if not differences_found:
        print("✓ Toutes les lignes sont identiques (différence de métadonnées seulement)")

print("\n" + "="*60)
# Nettoyage
spark.stop()
print("\n✓ Spark arrêté")