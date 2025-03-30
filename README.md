# Projet MapReduce - Analyse de Données Amazon

## Introduction
Ce projet vise à développer des programmes **MapReduce en Java** pour analyser des données issues du site Amazon. Trois requêtes principales sont traitées :
1. **Calculer le nombre de commentaires et la moyenne des étoiles** par catégorie de produit.
2. **Ordonner les résultats** par nombre de commentaires, dans un ordre décroissant.
3. **Identifier les 50 types de produits les plus achetés** parmi les clients ayant acheté au moins 10 articles.

_Les données n'ont pas été inclus dans ce repository, car elles sont volumineuses._
---
## Installation et Exécution
### Configuration de l'environnement
Avant de compiler et exécuter le programme, définissez les variables d'environnement suivantes :
```sh
export JAVA_HOME=/usr/gide/jdk-1.8
export PATH=${JAVA_HOME}/bin:${PATH}
export PDSH_RCMD_TYPE=ssh
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```

### Chargement des fichiers de données
Copiez les fichiers **AmazonReview*.tsv** dans votre répertoire Hadoop :
```sh
bin/hdfs dfs -put /home4/tn837970/Documents/M1/Systeme_distribue/TP_Note/AmazonRev/*tsv /user/tn837970/input
```

### Compilation et exécution du programme
Compiler le programme Java :
```sh
bin/hadoop com.sun.tools.javac.Main ExoX.java
```

Créer le fichier JAR :
```sh
jar cf ExoX.jar ExoX*.class
```

Exécuter le programme avec Hadoop :
```sh
bin/hadoop jar ExoX.jar ExoX /user/tn837970/input /user/tn837970/output
```

---
## Détails des Requêtes
### Requête 1 : Calcul du nombre de commentaires et de la moyenne des étoiles par catégorie
#### Mapper
Le Mapper extrait les informations suivantes :
- **Clé :** `product_category`
- **Valeur :** `1` (pour compter un commentaire) et `rating:X` (note attribuée)

#### Reducer
Le Reducer calcule :
- Le total des commentaires
- La somme des étoiles pour obtenir la moyenne

**Résultat :**
- Clé : `product_category`
- Valeur : `Nombre de commentaires` et `Moyenne des étoiles`
- Temps d'exécution : **7 minutes**

---
### Requête 2 : Classement par nombre de commentaires
#### Mapper
Même fonctionnement que pour la requête 1.

#### Reducer
- Utilisation d'une **TreeMap** pour trier les catégories par ordre décroissant du nombre de commentaires.

**Résultat :**
- Clé : `null` (le tri est réalisé dans la valeur)
- Valeur : `Catégories triées par nombre de commentaires`
- Temps d'exécution : **7 minutes**

---
### Requête 3 : Identification des 50 types de produits les plus achetés
#### Mapper
- **Clé :** `customer_id`
- **Valeur :** `product:X`, où `X` est la catégorie du produit acheté

#### Reducer
- Filtre les clients ayant acheté au moins **10 articles**
- Agrège les données par catégorie de produit
- Trie les catégories par nombre d’achats

**Résultat :**
- Clé : `Category`
- Valeur : `Quantity`
- Temps d'exécution : **7 minutes**

---
## Problèmes rencontrés et Optimisations
### Gestion des entrées
- Ignorer les premières lignes (en-têtes)
- Filtrer les valeurs nulles ou incorrectes

### Performances
- Utilisation d'une exécution **parallèle** via Hadoop
- Optimisation du tri avec **TreeMap**

### Limites
- La mémoire peut être un facteur limitant (interruption du processus MapReduce)

---
## Conclusion
Les programmes développés permettent de traiter efficacement de grandes quantités de données avec MapReduce. Pour aller plus loin, il serait intéressant :
- D’intégrer des **outils de visualisation** pour une meilleure interprétation des résultats.
- D'optimiser le traitement en utilisant un **Combiner** et plusieurs **nœuds** pour la répartition des charges.

---
## Auteurs
- **Meryem BELASSEL**
- **Thomas NICOLLE**



