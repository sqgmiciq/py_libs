'''
Created on Aug 12, 2020

@author: wakana_sakashita
'''

from setuptools import find_packages, setup
setup(
    name='spgmiciq',
    packages=find_packages(include=['spgmiciq']),
    version='0.1.1.5',
    description='CIQ API Python library',
    author='Wakana Sakashita',
    license='MIT',
    url='https://github.com/sqgmiciq/ciqd',
    install_requires=[            
          'pandas',
          'requests',
          'datetime',
          ],
    classifiers=[
        'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',      # Define that your audience are developers
        'Topic :: Software Development',
        'License :: OSI Approved :: MIT License',               # Again, pick a license
        'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3.6',
        ],
    
)
