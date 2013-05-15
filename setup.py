from setuptools import setup, find_packages


setup(
    name='ddd_common',
    version='0.1',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    url='',
    license='',
    zip_safe=False,
    include_package_data=True,
    author='Tomas Drencak',
    author_email='tomas@drencak.com',
    description='',
    tests_require=['nose']
)
