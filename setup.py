from setuptools import setup, find_packages

setup(
    name='s3file',
    version='0.0.0',
    url='https://github.com/nackjicholson/s3file',
    author='nackjicholson',
    description='Python file-like proxy for opening s3 files',
    long_description="See: http://github.com/nackjicholson/s3file",
    keywords='s3 file aws s3file fs',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'boto3>=1,<2',
        'botocore>=1,<2',
        'six>=1'
    ]
)
