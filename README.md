# 📘 Gestion des fichiers Cloud Storage & BigQuery

Ce projet permet d'automatiser le traitement des fichiers stockés dans un bucket Google Cloud Storage. Il inclut des étapes de téléchargement, nettoyage, validation, et transfert vers BigQuery. Les fichiers traités sont ensuite classés dans des dossiers appropriés.

---

## 📂 Structure du projet

- **Téléchargement** : Récupération des fichiers depuis `input/` dans Cloud Storage.
- **Validation** : Vérification de la structure et du format des données.
- **Nettoyage** : Traitement des valeurs manquantes et conversion des types.
- **Stockage** : Sauvegarde des fichiers nettoyés et déplacement vers les dossiers `clean/`, `done/` ou `error/`.
- **BigQuery** : (optionnel) Chargement des fichiers validés dans une table BigQuery.

---

## 🛠️ Explication des Fonctions Principales

### 🔍 `list_files_in_input()`
Liste tous les fichiers présents dans le dossier `input/` du bucket Cloud Storage.

### 📥 `download_blob(source_blob_name, destination_file_name)`
Télécharge un fichier depuis le bucket vers un fichier temporaire local.

### 📤 `upload_blob(content, destination_blob_name)`
Permet d'envoyer un fichier ou une chaîne de texte vers Cloud Storage.
- 📌 **Attention** : Cette fonction ne peut pas être incluse dans le traitement en chaîne continue, sauf si elle est utilisée pour stocker un fichier temporaire dans un sous-dossier (ex: `clean/` ou `done/`).

### ❌ `delete_file(filename)`
Supprime un fichier du bucket Cloud Storage s'il existe.

### 🧼 `clean_data(df)`
Nettoie les données en remplissant les valeurs manquantes et en s'assurant du bon format des colonnes clés.

### 🛡️ `validate_data(df)`
Vérifie si le DataFrame contient toutes les colonnes obligatoires et respecte les types de données attendus.

### 🔄 `process_cloud_file(cloud_file_path)`
Télécharge, nettoie et déplace les fichiers traités. Il gère également les erreurs :
- ✅ Fichiers propres -> `clean/` puis `done/`
- ❌ Fichiers erronés -> `error/`

### 🚀 `main()`
Orchestre l'ensemble du processus en traitant tous les fichiers de `input/` en boucle.

---

## 🔧 Exécution du Script

Lancer le script avec :
```sh
python script.py
