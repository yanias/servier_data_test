## HOW TO USE THIS DATA PIPELINE PROJECT


### Introdution

Ce projet est projet test dans le cadre d'un entretien chez *servier*
##structure du projet

Il peut avoir plusieurs manière d'oraganiser le projet mais j'ai fait le choix de cette structure

un dossier datas dans lequel il y'a 2 sous dossiers raw(contenant les fichiers bruts) et cleaned(contenant les fichiers cleanés)
un package src dans lequel se trouve un autre package data ou l'on trouve toutes les étapes
de etl(extract, tranform , load) avec les fonctions data_reader, data_cleaning ect ....
et un package test pour tester l'idempotence des fonctions utilisées.

Ce choix pour la lisibilié et aussi facilité les importance dans des modules comme dag

Le projet doit etre sur un repo git 

on pourrait également prendre en compte une étape de CI/CD pour tester l'intégration
de modification de code pour qu'on pourrait checker sur GCP CLOUD BUILD

# EXECUTION DU PROET

Pour exécuter ce  projet il faut suivre ces étapes ci-dessous:

1. **Créer un environnement virtuel avec  Python 3.8:**
   ```bash
   python3.8 -m venv venv
   ```

2. **Activer l'environnement virtuel:**
   ```bash
   source venv/bin/activate
   ```

3. **Installer les dépendances:**
   ```bash
   pip3 install -r requirements
   
4. **exécuter la fonction principale:**
   ```bash
   python -m src.main
   

## Lire de gros volume de données

```
Les élements à considérer sont le choix des technos opter pour les app qui 
font plus de parallelisme dans le traitement comme spark,dask
Stocker les fichiers sur des instances comme GCS , S3 
les élements en prendre en compte seront la connexion 
aux plateforme sur lesquelles les fichiers seront déposés 
```