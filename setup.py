from setuptools import setup

setup(
    name='elasticmapper',
    version='0.1',
    py_modules=['elastic_mapper'],
    install_requires=[
        'six',
    ],
    entry_points={
        'console_scripts': [
            'elasticmapper = elastic_mapper.cli.elasticmapper:cli',
        ],
    },
    extras_require={
        'cli':  ["Click"],
    }
)
