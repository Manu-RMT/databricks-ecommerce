# 🛒 E‑Commerce Lakehouse Pipeline (Databricks)

## 📌 Introduction

Ce projet implémente une architecture **Lakehouse en médaillon (Bronze → Silver → Gold)** pour traiter un jeu de données e‑commerce.  

L’objectif est de construire un pipeline complet, maintenable et orienté BI, basé sur :

- des notebooks Databricks orchestrés,

- des modules Python réutilisables,

- des tables Delta Lake optimisées pour l’analyse,

- un modèle dimensionnel (dimensions + fait),

- un **dashboard Databricks** pour la visualisation finale.

---

## 🗂️ Structure du Repository
 
 # 🛒 E‑Commerce Lakehouse Pipeline (Databricks)

## 📌 Introduction

Ce projet implémente une architecture **Lakehouse en médaillon (Bronze → Silver → Gold)** pour traiter un jeu de données e‑commerce.  

L’objectif est de construire un pipeline complet, maintenable et orienté BI, basé sur :

- des notebooks Databricks orchestrés,

- des modules Python réutilisables,

- des tables Delta Lake optimisées pour l’analyse,

- un modèle dimensionnel (dimensions + fait),

- un **dashboard Databricks** pour la visualisation finale.

---

## 🗂️ Structure du Repository
 
 # 🛒 E‑Commerce Lakehouse Pipeline (Databricks)

## 📌 Introduction

Ce projet implémente une architecture **Lakehouse en médaillon (Bronze → Silver → Gold)** pour traiter un jeu de données e‑commerce.  

L’objectif est de construire un pipeline complet, maintenable et orienté BI, basé sur :

- des notebooks Databricks orchestrés,

- des modules Python réutilisables,

- des tables Delta Lake optimisées pour l’analyse,

- un modèle dimensionnel (dimensions + fait),

- un **dashboard Databricks** pour la visualisation finale.

---

## 🗂️ Structure du Repository
 
 ![Image (7)_1770970611623.jpg](./Image (7)_1770970611623.jpg "Image (7)_1770970611623.jpg")
 
 ### 📁 `lib/` — Modules Python
- **config.py** : centralisation des chemins, paramètres, options Delta, constantes.
- **utils.py** : fonctions utilitaires (lecture/écriture Delta, nettoyage, logs, SCD, etc.).
### 📁 `notebooks/` — Pipeline Databricks
Chaque notebook correspond à une étape du pipeline en médaillon.
---
# 🧱 Architecture en Médaillon
## 🥉 Bronze — Ingestion des données brutes
**Notebook :** `01_bronze_ingestion_data`
Tables ingérées :
- brands  
- categories  
- customers  
- order_items  
- products  
**Principes :**
- ingestion *as-is*
- ajout de colonnes techniques (`ingestion_ts`, `source_file`)
- stockage en Delta Lake
- schéma brut conservé
---
## 🥈 Silver — Nettoyage & Normalisation
**Notebook :** `02_silver_transformation`
Transformations appliquées :
- nettoyage des chaînes (trim, accents, caractères spéciaux)
- normalisation des formats (dates, types, majuscules/minuscules)
- gestion des doublons
- harmonisation des clés
- ajout de colonnes de traçabilité :
 - `insert_ts`
 - `update_ts`
 - `is_current`
Tables Silver générées :
- `slv_ecommerce_brands`
- `slv_ecommerce_categories`
- `slv_ecommerce_customers`
- `slv_ecommerce_order_items`
- `slv_ecommerce_products`
---
## 🥇 Gold — Modèle Dimensionnel
### 📘 Dimensions
**Notebook :** `03_gold_dimensions_tables`
Tables créées :
- `dim_ecommerce_customers`
- `dim_ecommerce_products`
- `dim_ecommerce_brands`
- `dim_ecommerce_categories`
Caractéristiques :
- clés substituts
- colonnes business enrichies
- gestion SCD si nécessaire
- tables optimisées pour la BI
---
### 📗 Faits
**Notebook :** `03_gold_facts_table`
Table créée :
- `fact_ecommerce_order_items`
Grain : **ligne d’article d’une commande**
Contenu :
- clés étrangères vers les dimensions
- mesures : quantité, prix unitaire, montant total
- dates de commande / livraison
- optimisation Delta (Z‑Order, partitionnement si pertinent)
---
# 📊 Dashboard Databricks — Visualisation Finale
Un **dashboard Databricks** est construit à partir des tables Gold afin de fournir une vue analytique complète de l’activité e‑commerce.
### Indicateurs clés :
- Total des ventes  
- Nombre de commandes  
- Quantité vendue par produit  
- Top catégories / top marques  
- Analyse clients (segmentation, récurrence, panier moyen)  
- Analyse temporelle (jour, mois, année)
### Sources du dashboard :
- `fact_ecommerce_order_items`
- `dim_ecommerce_products`
- `dim_ecommerce_customers`
- `dim_ecommerce_brands`
- `dim_ecommerce_categories`
Le dashboard permet une **analyse interactive** directement dans Databricks SQL.
---
# ⚙️ Notebook 00 — Setup & Initialisation
**Notebook :** `00_setup`
Rôle :
- création des zones `/bronze`, `/silver`, `/gold`
- configuration des paramètres globaux
- import des modules Python (`config`, `utils`)
- vérification de l’environnement
---
# 🔍 Notebook — Queries Silver Zone
Notebook dédié à l’exploration et la validation des tables Silver :
- profiling
- contrôles qualité
- vérification des relations
- tests de cohérence
---
# 🚀 Workflow d’Exécution
1. **00_setup**
2. **01_bronze_ingestion_data**
3. **02_silver_transformation**
4. **03_gold_dimensions_tables**
5. **03_gold_facts_table**
6. **Dashboard Databricks (visualisation finale)**
---
# 🎯 Objectifs du Projet
- Construire un pipeline **fiable, traçable et maintenable**
- Appliquer les bonnes pratiques du **Delta Lake**
- Produire un modèle **dimensionnel** prêt pour l’analyse
- Fournir un **dashboard analytique** pour la prise de décision
- Faciliter l’intégration avec des outils BI
---
 
 