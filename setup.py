from setuptools import setup, find_packages

packages = find_packages(
        where='.',
        include=['kktrade*']
)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='kktrade',
    version='1.2.2',
    description='python libraries for trade.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kazukingh01/kktrade",
    author='Kazuki Kume',
    author_email='kazukingh01@gmail.com',
    license='Public License',
    packages=packages,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Private License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'kkpsgre @ git+https://github.com/kazukingh01/kkpsgre.git@c802ea4cb48bdd94929d68531525c38b515e70cc',
        'pandas==2.2.1',
        'numpy==1.26.4',
        'joblib==1.3.2',
        'requests==2.32.0',
        'tqdm==4.66.4',
        'websockets==12.0',
        'httpx==0.27.0',
        'beautifulsoup4==4.12.3',
        'playwright==1.46.0',
        'fastapi==0.112.0',
        'uvicorn==0.30.5',
    ],
    python_requires='>=3.12.2'
)
