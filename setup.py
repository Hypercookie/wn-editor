from setuptools import setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
setup(
    name='wn-editor',
    version='0.0.3',
    packages=['editor'],
    url='https://github.com/Hypercookie/wn-editor',
    license='MIT',
    author='Jannes Müller',
    author_email='jannes@mlrjs.de',
    python_requires='>3.5',
    description='Make wordnets editable with wn and this editor extension.',
    install_requires=[
        "wn"
    ],
    long_description=long_description,
    long_description_content_type='text/markdown'
)
