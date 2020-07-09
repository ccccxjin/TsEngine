import setuptools

with open('README.md', 'r') as fh:
    readme = fh.read()


setuptools.setup(
    name='tsengine',
    version='1.0.3',
    url='https://github.com/ccccxjin/TsEngine',
    description='python taosdb extension package',
    long_description_content_type='text/markdown',
    long_description=readme,
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
