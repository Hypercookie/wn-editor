from setuptools import setup

setup(
    name='wn-editor',
    version='0.0.1',
    packages=['editor'],
    url='https://github.com/Hypercookie/wn-edtior',
    license='MIT',
    author='Jannes Müller',
    author_email='jannes@mlrjs.de',
    python_requires='>3.5',
    description='Make wordnets editable with wn and this editor extension.',
    install_requires=[
        "wn"
    ]
)
