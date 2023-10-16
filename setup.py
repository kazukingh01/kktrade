from setuptools import setup, find_packages

packages = find_packages(
        where='.',
        include=['kktrade*']
)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='kktrade',
    version='1.0.0',
    description='augmentation wrapper package for albumentations',
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
        'psycopg2-binary==2.9.5',
        'pandas==1.5.3',
        'numpy==1.24.2',
        'joblib==1.3.2',
        'requests==2.31.0',
        'tqdm==4.66.1',
    ],
    python_requires='>=3.11.2'
)
