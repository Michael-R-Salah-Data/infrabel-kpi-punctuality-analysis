import requests
from tqdm import tqdm
from pathlib import Path


def prepare_download(datasets, output_dir="data/raw/", file_type="parquet"):
    """
    Prepares download parameters by iterating over a dictionary of datasets 
        {dataset_name : dataset_url}.
    Allows selecting the file type and building the output path for each dataset 
        in the dictionary.
    Uses pathlib to handle path differences between Windows and Linux.

    Args: 
        datasets (dict): {dataset_name : dataset_url} dictionary of datasets to download.
        output_dir (str): path to the output directory. Default: "data/raw/".
        file_type (str): extension of the downloaded files. Leading dots and whitespaces are stripped, 
            and a dot is re-added internally. Default: "parquet".

    Returns:
        list: list of tuples (dataset_url, output_path, dataset_name, 
            dataset_extension, output_dir).

    """
    datasets_list = []
    # Automatically normalizes file_type: strips whitespaces and any leading dot, and converts to lowercase
    file_type = file_type.strip().lstrip(".").lower()
    dir_path = Path(output_dir)

    for dataset_name, dataset_url in datasets.items():
        full_file_name = f"{dataset_name}.{file_type}"
        output_path = dir_path / full_file_name
        datasets_list.append((dataset_url, str(output_path), dataset_name, file_type, output_dir))
    return datasets_list


def check_output_dir(output_dir): 
    """
    Checks that the output directory exists before downloading.

    If the directory does not exist, prompts the user for confirmation before creating it, 
        to allow detection of potential typos in the provided path.

    If the user declines, the download is cancelled. 
   
    Args: 
        output_dir (str): path to the output directory.
            Typically provided by the orchestrating function run_download().

    Returns: 
        None

    Side Effects:
        If the directory already exists: None.
        If the directory does not exist and the user confirms creation:
            Creates the output directory at the path specified by output_dir.

    Raises: 
        SystemExit: if the directory does not exist and the user declines its creation,
            the download is interrupted.

    """
    # Verify if the path exists 
    dir_path = Path(output_dir)
    if dir_path.exists() and dir_path.is_dir():
        # Path exists and is a valid directory : no action required
        return None
    # Verify that tbe path points to an existing file
        # Edge case: a file with the same name as the target directory already exists
    elif dir_path.exists() and not dir_path.is_dir():
        raise FileExistsError(
            f"Name conflict: {output_dir} already exists but is a file. "
            "The system cannot create a directory with the same as this file."
        )
    # If the repertory does not exist, prompt the user to create it  
    else:
        user_mkdir = input(f"The directory {output_dir} doesn't exist. Do you want to create it ? (y/n)")
        # If the user confirms, create the directory and return nothing
        if user_mkdir.strip().lower() == 'y':
            dir_path.mkdir(parents=True)
            return None
        # If the user declines, abord the download by raising a SystemExit exception
        else:
            raise SystemExit("Download canceled by the user. " 
            "Verify the path to the output directory.")
            

def download_dataset(
        dataset_url, 
        output_path, 
        dataset_name, 
        file_type,
        referer=None, 
        chunk_size=10240
        ):
   
    """
    Download a dataset from a URL:
        - Connects to the dataset URL. 
        - Chunks the dataset to handle large files. 
        - Controls the connection timeout and the reception timeout for each chunk.
        - Writes the downloaded dataset to a file in the target directory.
        - Creates a session, a User-Agent, and a referer to handle downloads
            from dynamic sessions.

    Args: 
        Typically provided by the orchestrating function run_download():
            dataset_url (str): URL of the dataset.
            output_path (str): path to the download file.
            dataset_name (str): name of the file.
            file_type (str): file extension.
        chunk_size (int): size of each chunk. Default: 10240.

    Returns: 
        file_name (str): full name of the downloaded file (dataset_name.file_type)

    Side Effects:
        Writes the downloaded full name to a manifest file in the target directory.
    """
    # Create a session to store potential cookies
    session = requests.Session()

    # Define headers to mimic browser behavior
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
    })

    print(f"Starting downlod : {dataset_name}\n")

    try: 
        # If a referer is provided, request a session control
        if referer: 
            session.headers.update({'Referer' : referer})
            session.get(referer, timeout=15)
            print(f"Session initialized via the referer for : {dataset_name}\n")
            
        # Download the dataset with a timeout to control the server connection time (10)
            # and the reception time between each chunk (120)
        response = session.get(dataset_url, stream=True, timeout=(10, 120))
        response.raise_for_status()
           
        # Chunk the dataset 
        with open(output_path, "wb") as f:
            # Write the file 
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
            tqdm.write(f"{dataset_name} downloaded to {output_path}")
            file_name = f"{dataset_name}.{file_type}"
            return file_name
    
    except requests.exceptions.RequestException as error: 
            tqdm.write(f"Error downloading {dataset_name} : {error}")

    finally:
        session.close()


def generate_file_registry(
        downloaded_datasets, 
        output_dir, 
        registry_name="manifest.txt"
        ):
    """
    Export the list of downloaded files as a manifest.txt file
        created in the target directory.
        
    Args:
        downloaded_datasets (list[str]): list of downloaded files. 
            Each element is a file name formatted as "{dataset_name}.{file_type}".
            Typically provided by the orchestrating functions.
        output_dir (str): path to the target directory.
        registry_name (str): name of the created file. Default: "manifest.txt".
    
    Returns:
        None

    Side Effects:
        Creates a manifest file in the target directory.

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
    Orchestrates the four preceding functions by calling them in the correct sequence,
        collecting the results returned by each function and passing them as parameters to the next.
    Manages tqdm across the entire process and displays a progress bar tracking the number of datasets 
        downloaded out of the total in the `datasets` dictionary.
        Note: individual file sizes are not tracked, as some datasets are dynamically generated 
        at download time and have an initial size of 0. Each dataset therefore counts as one unit 
        of progress regardless of its size or download time.

    Args: 
        datasets (dict[str, str]): dictionary of datasets to download {dataset_name : URL}.
        output_dir (str): path to the target directory. Default: "data/raw/".
        file_type (str): file format of the downloaded datasets (e.g., "parquet", "csv", "xlsx", "zip"). 
            Expected format: string without a leading dot. Default: "parquet".
        registry_name (str): name of the manifest file. Default: "manifest.txt".

    Returns:
        None

    Side Effects:
        Orchestrating the sequencing of the following functions:
            - prepare_download()
            - check_output_dir()
            - download_dataset()
            - generate_file_registry()
        Displays a download progress bar.
    """
    # Check that the dictionary exists and contains key-value pairs
    if not datasets:
        raise ValueError("No datasets provided")
    
    # Prepare the datasets
    prepared_datasets = prepare_download(datasets, output_dir, file_type)

    # Call the check_output_dir() function, verify that the output directory exists 
        # and do not have a name conflict
    dir_to_check = prepared_datasets[0][-1]
    check_output_dir(dir_to_check)

    # Download each element in the list and collect the downloaded file names in a separate list
    # Create and update a tqdm progress bar
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

    # Create a manifest of the downloaded datasets
    generate_file_registry(downloaded_datasets, output_dir, registry_name)

    # Display the list of download failures, if any
    if failed_downloads:
        print("\nFailed Downloads: ")
        for f in failed_downloads:
            print(f"- {f}\n")

