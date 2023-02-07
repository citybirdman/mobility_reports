from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in mobility_reports/__init__.py
from mobility_reports import __version__ as version

setup(
	name="mobility_reports",
	version=version,
	description="Customised reports for Mobility",
	author="Aerele Technologies Private Limited",
	author_email="vignesh@aerele.in",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
