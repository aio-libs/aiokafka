import os
import re
import sys
from setuptools import setup, find_packages


install_requires = ['kafka-python>=0.9.3-dev']

PY_VER = sys.version_info

if PY_VER >= (3, 4):
    pass
elif PY_VER >= (3, 3):
    install_requires.append('asyncio')
else:
    raise RuntimeError("aiokafka doesn't suppport Python earllier than 3.3")


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()

extras_require = {'snappy': ['python-snappy>=0.5'], }


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'aiokafka', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in aiokafka/__init__.py')

classifiers = [
    'License :: OSI Approved :: Apache Software License',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Operating System :: POSIX',
    'Operating System :: MacOS :: MacOS X',
    'Environment :: Web Environment',
    'Development Status :: 4 - Beta',
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
]


setup(name='aiokafka',
      version=read_version(),
      description=('Kafka integration with asyncio.'),
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.rst'))),
      classifiers=classifiers,
      platforms=['POSIX'],
      author='Andrew Svetlov',
      author_email='andrew.svetlov@gmail.com',
      url='http://aiokafka.readthedocs.org',
      download_url='https://pypi.python.org/pypi/aiokafka',
      license='Apache 2',
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data = True)
