[![PyPI version](https://img.shields.io/pypi/v/wn-editor.svg)](https://pypi.org/project/wn-editor/)
# wn-editor
> A extension for the popular wn package, making wordnets editable from python!

## Motivation
Currently the [wn python package](https://github.com/goodmami/wn) does not support editing the imported wordnets. For some usecases editing the wordnets or creating new ones on the fly is very important. This package aims to be an extension of wn and provides an API for editing wordnets.
## Documentation
This package integrates directly into the wordnet database maintained by wn, and thus allowes to change data and not only provide a changed view.
This is done by using different editor classes for the various wordnet components.
### Quickstart
1. Install this package using `pip install wn-editor`
2. import the package (and wn) like so
```python
import wn 
from wn_editor.editor import LexiconEditor

# Get and editor for an installed lexicon
lex_edit = LexiconEditor('odenet')
# Use differnet methods to create and edit synsets
lex_edit.create_synset()
    .add_word('auto')
    .set_hypernym_of(wn.synsets('mercedes')[0])
```
3. Read the wiki for more detailed info about the available methods and classes.
