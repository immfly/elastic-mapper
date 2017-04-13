from setuptools import setup

setup(
    name='elasticmapper',
    version='0.1',
    packages=['elastic_mapper'],
    install_requires=[
        'six',
        'arrow',
    ],
    entry_points={
        'console_scripts': [
            'elasticmapper = elastic_mapper.cli.elasticmapper:cli',
        ],
    },
    extras_require={
        'cli': ["Click", "pygments"],
    }
)
