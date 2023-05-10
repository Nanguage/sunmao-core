from setuptools import setup, find_namespace_packages
import re


classifiers = [
    "Development Status :: 3 - Alpha",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Developers",
]


keywords = [
    'Workflow',
]


URL = "https://github.com/Nanguage/sunmao-core"


def get_version():
    with open("sunmao/core/__init__.py") as f:
        for line in f.readlines():
            m = re.match("__version__ = '([^']+)'", line)
            if m:
                return m.group(1)
        raise IOError("Version information can not found.")


def get_long_description():
    return f"See {URL}"


def get_requirements_from_file(filename):
    requirements = []
    with open(filename) as f:
        for line in f.readlines():
            line = line.strip()
            if len(line) == 0:
                continue
            if line and not line.startswith('#'):
                requirements.append(line)
    return requirements


def get_install_requires():
    return get_requirements_from_file('requirements.txt')


requires_test = [
    'pytest', 'pytest-cov', 'pytest-order',
    'pytest-asyncio', 'flake8', 'mypy'
]


setup(
    name='sunmao-core',
    author='Weize Xu',
    author_email='vet.xwz@gmail.com',
    version=get_version(),
    license='MIT',
    description='Sunmao is an workflow engine for build workflow tools.',
    long_description=get_long_description(),
    keywords=keywords,
    url=URL,
    packages=find_namespace_packages(include=['sunmao.*']),
    include_package_data=True,
    zip_safe=False,
    classifiers=classifiers,
    install_requires=get_install_requires(),
    extras_require={
        'test': requires_test,
    },
    python_requires='>=3.8, <4',
)
