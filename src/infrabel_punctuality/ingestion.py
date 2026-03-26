import requests
from tqdm import tqdm
from pathlib import Path


def prepare_download(datasets, output_dir="data/raw/", file_type="parquet"):
    """
    Prépare les paramètres de téléchargements en itérant sur un dictionnaire de datasets {nom : url}.
    Permet de choisir le type de fichiers à télécharger et crée le path du output.
    Utilise pathlib pour gérer les différences de path entre Windows et Linux.

    Arguments: 
        datasets (dict): dictionnaire {dataset_name : dataset_url} des datasets à télécharger.
        output_dir (str): path vers le répertoire du fichier. Défaut: "data/raw/".
        file_type (str): extension des fichiers téléchargés. Défaut: ".parquet".

    Returns:
        liste: liste de tuples (dataset_url, output_path, dataset_name, dataset_extension, output_dir)

    """
    datasets_list = []
    # nettoyage automatique : enlever un éventuel point dans file_type et tout mettre en minuscules 
    file_type = file_type.lstrip(".").lower()
    dir_path = Path(output_dir)

    for dataset_name, dataset_url in datasets.items():
        full_file_name = f"{dataset_name}.{file_type}"
        output_path = dir_path / full_file_name
        datasets_list.append((dataset_url, str(output_path), dataset_name, file_type, output_dir))
    return datasets_list


def check_output_dir(output_dir): 
    """
    Vérifie que le répertoire de destination existe avant le téléchargement.

    Si le répertoire n'existe pas, demande confirmation à l'utilisateur avant de le créer,
    pour permettre la détection d'éventuelles fautes dans le path entré en paramètre.
    Si l'utilisateur refuse la création du répertoire, le téléchargement est annulé.

    Arguments: 
        output_dir (str): path vers le répertoire de destination. 
            Généralement fourni par la fonction orchestratrice run_download()

    Returns: 
        Si le répertoire exite déjà : None

    Side effects:
        Si le répertoire n'existe pas et que l'utilisateur accepte la création : 
            Crée un répertoire de destination au path indiqué par output_dir

    Raises: 
        SystemExit: si le répertoire n'existe pas et que l'utilisateur refuse la création du répertoire,
            le téléchagerment est interrompu.

    """
    # vérifier que le chemin existe 
    dir_path = Path(output_dir)
    if dir_path.exists() and dir_path.is_dir():
        # tout est ok : le chemin existe et est celui d'un répertoire => ne rien faire
        return None
    # vérifier que le chemin est celui d'un fichier qui existe déjà
        # cas bizarre : il existe un fichier qui porte le même nom que le répertoire de destination à créer
    elif dir_path.exists() and not dir_path.is_dir():
        raise FileExistsError(
            f"Conflit de noms : {output_dir} existe déjà mais c'est un fichier. "
            "Le système ne peut pas créer un dossier portant le même nom que ce fichier."
        )
    # si le répertoire n'existe pas, proposer à l'utilisateur de le créer    
    else:
        user_mkdir = input(f"Le répertoire {output_dir} n'existe pas. Voulez-vous le créer ? (y/n)")
        # si l'utilisateur accepte, créer le répertoire et ne rien retourner
        if user_mkdir.strip().lower() == 'y':
            dir_path.mkdir(parents=True)
            return None
        # si l'utilisateur refuse, arrêter le téléchargement en levant une exception SystemExit
        else:
            raise SystemExit("Téléchargement annulé par l'utilisateur. Vérifier le chemin du répertoire de destination")
            

def download_dataset(
        dataset_url, 
        output_path, 
        dataset_name, 
        file_type,
        referer=None, 
        chunk_size=10240
        ):
    # idée : si l'utilisateur le précise, il peut aussi modifier les deux valeurs du timeout du request
   
    """
    Télécharge un dataset à partir d'une url :
        - Se connecte à l'url du dataset 
        - Chunke le dataset pour gérer les gros volumes
        - Contrôle le temps de connection au serveur et le temps de réception de chaque chunk
        - Écris un fichier contenant le dataset téléchargé dans le répertoire de destination
        - Crée une session, un User-Agent et un referer pour gérer les téléchargements depuis les sessions dynamiques

    Arguments: 
        Généralement fournis par la fonction orchestratrice run_download():
            dataset_url (str): url du dataset
            output_path (str): chemin vers le fichier téléchargé
            dataset_name (str): nom du fichier
            file_type (str): extension du fichier
        chunk_size (int): taille de chaque chunk. Défaut: 10240.

    Returns: 
        file_name (str): nom complet du fichier téléchargé (dataset_name.file_type)

    Side effects:
        Écris le contenu téléchargé dans un fichier créé dans le répertoire de destination.
    """
    # Créer une session pour mémoriser les éventuels cookies
    session = requests.Session()

    # Définir des headers et imiter le comportement d'un navigateur 
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
    })

    print(f"Début du téléchargement : {dataset_name}\n")

    try: 
        # si referer, demander un cookie de session
        if referer: 
            session.headers.update({'Referer' : referer})
            session.get(referer, timeout=15)
            print(f"Session initialisée via le referer pour : {dataset_name}\n")
            
        # téléchager le dataset et mettre un "timer" pour contrôler le temps de connection au serveur (10)
            # et le temps de réception entre chaque chunk (120)
        response = session.get(dataset_url, stream=True, timeout=(10, 120))
        response.raise_for_status()
           
        # chunker le dataset pour gérer sa taille
        with open(output_path, "wb") as f:
            # écrire le fichier 
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
            tqdm.write(f"{dataset_name} téléchargé dans {output_path}")
            file_name = f"{dataset_name}.{file_type}"
            return file_name
    
    except requests.exceptions.RequestException as error: 
            tqdm.write(f"Erreur lors du téléchargement de {dataset_name} : {error}")

    finally:
        session.close()


def generate_file_registry(
        downloaded_datasets, 
        output_dir, 
        registry_name="manifest.txt"
        ):
    """
    Exporte la liste des fichiers téléchargés sous la forme d'un fichier manifest.txt créé dans le répertoire de destination
        
    Arguments:
        downloaded_datasets (list[str]): liste des fichiers téléchargés. Nécessairement fournie par la fonction orchestratrice.
        output_dir (str): le chemin du répertoire de destination
        registry_name (str): le nom du fichier créé. Défaut: "manifest.txt".
    
    Returns:
        None

    Side effects:
        Crée un fichier manifest.txt dans le répertoire de destination

    """
   
    registry_path = Path(output_dir) / registry_name
    content = "\n".join(downloaded_datasets) + "\n"
    
    registry_path.write_text(content, encoding="utf-8")


def run_download(
        datasets, 
        output_dir="data/raw/", 
        file_type="parquet",  
        registry_name="manifest.txt",
        referer=None
        ):
    """
    Orchestre les quatre fonctions précédentes en les appelant dans la bonne séquence, 
        en collectant les résultats retournés par une fonction et en les passant en paramètres de la fonction suivante.
    Gère tqdm sur l'ensemble et affiche une barre de progression des téléchargements.

    Arguments: 
        datasets (dict{str : str}) : dictionnaire contenant la liste des datasets à télécharger {name : url}
        output_dir (str) : le chemin vers le répertoire de destination. Défaut : "data/raw/".
        file_type (str) : le format de fichiers des datasets téléchargés (parquet, csv, xlsx)? Défaut : "parquet".
        registry_name (str) : nom du fichier qui contiendra le manifeste des téléchargements. Défaut : "manifest.txt".

    Returns:
        None

    Side effect:
        Orchestre le séquençage des fonctions: 
            - prepare_download()
            - check_output_dir()
            - download_dataset()
            - generate_file_registry()
        Affiche une barre de téléchargement
    """
    # vérifier que le dictionnaire existe et n'est pas vide
    if not datasets:
        raise ValueError("No datasets provided  ")
    
    # préparer les datasets
    prepared_datasets = prepare_download(datasets, output_dir, file_type)

    # vérifier que le répertoire existe
    dir_to_check = prepared_datasets[0][-1]
    check_output_dir(dir_to_check)

    # downloader chaque élément de la liste et mettre le nom de chaque dataset dans une autre liste
    # créer et faire avancer une barre de progression des téléchargements
    downloaded_datasets= []
    failed_downloads = []
    with tqdm(
            total = len(prepared_datasets),
            unit='file',
            unit_scale=True,
            desc="Downloading datasets"
            ) as pbar:  
        for url, path, name, ext, _ in prepared_datasets:
            try:
                file_name = download_dataset(url, path, name, ext, referer=referer)
                if file_name is not None:
                    downloaded_datasets.append(file_name)
                else:
                    failed_downloads.append(name)

            except Exception as e:
                tqdm.write(f"Download failed for {name} : {e}")
                failed_downloads.append(name)
            
            finally:
                pbar.update(1)

    # créer un manifeste des datasets téléchargés
    generate_file_registry(downloaded_datasets, output_dir, registry_name)

    #afficher la liste des échecs de téléchargement si elle existe:
    if failed_downloads:
        print("\nFailed Downloads: ")
        for f in failed_downloads:
            print(f"- {f}\n")

