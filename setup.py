from setuptools import setup, find_packages

setup(
    name="infrabel_punctuality",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "requests",
        "tqdm",
        "pandas"
    ]
)