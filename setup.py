from setuptools import setup, find_packages

packages = find_packages(
        where='.',
        include=['kktrade*']
)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='kktrade',
    version='1.1.0',
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
        'kkpsgre @ git+https://github.com/kazukingh01/kkpsgre.git@f201baa0ba37277b216fa5bdda819fc934273384',
        'pandas==1.5.3',
        'numpy==1.24.2',
        'joblib==1.3.2',
        'requests==2.31.0',
        'tqdm==4.66.1',
        'websockets==11.0.3',
        'httpx==0.25.0',
        'beautifulsoup4==4.12.3',
        'pytest-playwright==0.4.4',
        'playwright==1.41.2',
    ],
    python_requires='>=3.11.2'
)
