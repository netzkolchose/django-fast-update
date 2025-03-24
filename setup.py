from setuptools import setup, find_packages

with open('README.md', 'r') as f:
    long_description = f.read()

def get_version(path):
    with open(path) as f:
        for line in f:
            if line.startswith('__version__'):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
        raise RuntimeError('Unable to find version string.')

setup(
    name='django-fast-update',
    packages=find_packages(exclude=['example']),
    include_package_data=True,
    install_requires=['Django>=3.2'],
    version=get_version('fast_update/__init__.py'),
    license='MIT',
    description='Faster db updates for Django using UPDATE FROM VALUES sql variants.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='netzkolchose',
    author_email='j.breitbart@netzkolchose.de',
    url='https://github.com/netzkolchose/django-fast-update',
    download_url='https://github.com/netzkolchose/django-fast-update/archive/v0.2.4.tar.gz',
    keywords=['django', 'bulk_update', 'fast', 'update', 'fast_update'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries',
        'Framework :: Django',
        'Framework :: Django :: 3.2',
        'Framework :: Django :: 4.2',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11'
    ],
)
