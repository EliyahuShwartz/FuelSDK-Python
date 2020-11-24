from setuptools import setup, find_packages

with open('README.md', encoding="utf-8") as f:
    readme = f.read()

setup(
    version='1.5.87',
    name='Salesforce-FuelSDK-Extended',
    description='Salesforce Marketing Cloud Fuel SDK for Python',
    long_description=readme,
    long_description_content_type="text/markdown",
    author='ExactTarget,Eliyahu Schwarz',
    py_modules=['ET_Client'],
    packages=find_packages(),
    url='https://github.com/EliyahuShwartz/FuelSDK-Python',
    license='MIT',
    install_requires=[
        'pyjwt>=1.5.3',
        'requests>=2.18.4',
        'suds-jurko==0.6',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 3.3',
    ],
)
