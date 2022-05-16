from __future__ import annotations

import inspect
from enum import IntEnum
from typing import overload, Optional, Any

import wn
from wn import Synset
from wn._add import logger
from wn._db import connect
from wn._queries import get_modified
from wn.lmf import (
    Metadata,
)


# Utils
def _mod_internal(f, *args, **kw):
    args[0].set_modified()
    return f(*args, **kw)


def _modifies_db(func):
    return _dec(func, _mod_internal)


def get_row_id(table, arg: dict[str, Any]) -> int:
    condition = " AND ".join([f"{i}=?" for i in arg])
    ar = [arg[i] for i in arg]
    query = f"SELECT rowid FROM {table} WHERE {condition}"
    with connect() as conn:
        res = conn.cursor().execute(query, tuple(ar)).fetchall()
        if res is not None:
            if len(res) > 1:
                logger.warn(
                    "More then one rowid returned while matching rowids "
                    "(thats probably coused by duplicate IDs in the same lexicon"
                )
            elif len(res) < 1:
                logger.warn("No rowid returned while searching for rowids")
            else:
                if res[0] is not None:
                    return res[0][0]


##############
def _fix(args, kwargs, sig):
    """
    Fix args and kwargs to be consistent with the signature
    """
    ba = sig.bind(*args, **kwargs)
    ba.apply_defaults()  # needed for test_dan_schult
    return ba.args, ba.kwargs


# TODO Rant about pycharm (even this black magic below does NOT fix auto completion if decorators are involved.
def _dec(func, caller, extras=(), kwsyntax=False):
    sig = inspect.signature(func)

    def fun(*args, **kw):
        if not kwsyntax:
            args, kw = _fix(args, kw, sig)
        return caller(func, *(extras + args), **kw)

    fun.__name__ = func.__name__
    fun.__doc__ = func.__doc__
    fun.__wrapped__ = func
    fun.__signature__ = sig
    fun.__qualname__ = func.__qualname__
    # builtin functions like defaultdict.__setitem__ lack many attributes
    try:
        fun.__defaults__ = func.__defaults__
    except AttributeError:
        pass
    try:
        fun.__kwdefaults__ = func.__kwdefaults__
    except AttributeError:
        pass
    try:
        fun.__annotations__ = func.__annotations__
    except AttributeError:
        pass
    try:
        fun.__module__ = func.__module__
    except AttributeError:
        pass
    try:
        fun.__dict__.update(func.__dict__)
    except AttributeError:
        pass
    return fun


class IliStatus(IntEnum):
    presupposed = 1
    proposed = 2
    active = 3


class RelationType(IntEnum):
    antonym = 1
    causes = 2
    derivation = 3
    entails = 4
    holo_member = 5
    holo_part = 6
    hypernym = 7
    hyponym = 8
    mero_member = 9
    mero_part = 10
    meronym = 11
    pertainym = 12
    also = 13
    attribute = 14
    domain_region = 15
    domain_topic = 16
    exemplifies = 17
    has_domain_region = 18
    has_domain_topic = 19
    holo_substance = 20
    instance_hypernym = 21
    instance_hyponym = 22
    is_caused_by = 23
    is_entailed_by = 24
    is_exemplified_by = 25
    mero_substance = 26
    participle = 27
    similar = 28


SET_MOD_QUERY = """

UPDATE lexicons SET modified=1 WHERE rowid=?

"""
LEXICON_ID_QUERY = """

SELECT id FROM lexicons WHERE rowid=?

"""


def get_artificial(rowid: int) -> bool:
    query = """
    SELECT metadata FROM lexicons WHERE rowid = ?
    """
    with connect() as conn:
        res = conn.cursor().execute(query, (rowid,)).fetchall()
        if res and res[0] and res[0][0] is not None and "note" in dict(res[0][0]):
            return "_.artificial" in dict(res[0][0])["note"]
        else:
            return False


def reset_all_wordnets(delete_artificial=False):
    """

    Resets all wordnets which have been modified. By re-downloading them (or using cache).
    Warning: This will delete ALL modifications made and is __not__ reversible

    """
    pr = wn.lexicons()
    print("Installed: ")
    get_wordnet_overview()
    for prj in pr:
        rowid = get_row_id("lexicons", {"id": prj.id, "version": prj.version})
        artificial = get_artificial(rowid)
        modified = get_modified(rowid)
        if modified and (not artificial or delete_artificial):
            wn.remove(f"{prj.id}:{prj.version}")
            if not artificial:
                wn.download(f"{prj.id}:{prj.version}")


def get_wordnet_overview():
    """

    Prints an overview over all wordnets

    """
    for lex in wn.lexicons():
        rowid = get_row_id("lexicons", {"id": lex.id, "version": lex.version})

        print(
            f"{lex.id}:{lex.version}\t{lex.label}"
            + ("\tModified" if get_modified(rowid) else "\t")
            + ("\tArtificial" if get_artificial(rowid) else "\t")
        )


def _get_valid_sense_id(entry_id: int, form: str = "unkown") -> str:
    get_all_sense_id_of_entry_query = """
    
    SELECT max(cast(replace(id,?,'') as unsigned)) FROM senses WHERE entry_rowid = ? and  id like ?
    
    """
    with connect() as conn:
        cur = conn.cursor()
        s = f"w_{form}_"
        cur.execute(get_all_sense_id_of_entry_query, (s, entry_id, s + "%"))
        res = cur.fetchall()
        if res and res[0] and res[0][0] is not None:
            return s + str(res[0][0] + 1)
        else:
            return s + "0"


def _get_valid_entity_id() -> str:
    query = """
    
    SELECT max(cast(replace(id,'w','') as unsigned)) FROM entries WHERE id like 'w%'

    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(query)
        res = cur.fetchall()
        if res and res[0]:
            return "w" + str(res[0][0] + 1)
        else:
            return "w0"


def _get_valid_synset_id(lex_rowid: int) -> str:
    query = """
    SELECT max(cast(replace(synsets.id,?,'') as unsigned)) FROM synsets WHERE lexicon_rowid = ? and id like ?
    """
    with connect() as conn:
        cur = conn.cursor()
        s = _get_lex_name_from_lex_id(lex_rowid) + "-"
        res = cur.execute(query, (s, lex_rowid, s + "%")).fetchall()
        if res and res[0] and res[0][0] is not None:
            return s + str(res[0][0] + 1)
        else:
            return s + "0"


def _get_lex_name_from_lex_id(lex_id) -> str:
    query = """

    SELECT id from lexicons WHERE rowid = ?

    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(query, (lex_id,))
        res = cur.fetchall()
        if res and res[0]:
            return str(res[0][0])


def _get_ili_rowid_from_id(ili_id: str) -> int:
    query = """
    
    SELECT rowid FROM ilis WHERE id = ?
    
    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(query, (ili_id,))
        res = cur.fetchall()
        if res and res[0]:
            return int(res[0][0])


def _get_all_lexicon_row_ids() -> list[int]:
    query = """
        SELECT rowid FROM lexicons
    """
    with connect() as conn:
        res = conn.cursor().execute(query, ()).fetchall()
        return [r[0][0] for r in res if res[0]]


def _get_lex_id_from_row(rowId) -> str | None:
    with connect() as conn:
        cur = conn.cursor()

        cur.execute(LEXICON_ID_QUERY, (rowId,))
        i = cur.fetchall()[0]
        if i:
            return i[0]


def _get_row_id_from_lex(lex_id) -> int | None:
    get_query = """
    
    SELECT rowid FROM lexicons WHERE id =?
    
    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(get_query, (lex_id,))
        res = cur.fetchall()
        if res and res[0]:
            return int(res[0][0])


def _get_valid_ili_id() -> str:
    query = """
    
    SELECT max(cast(replace(id,'i','') as unsigned)) as ids from ilis
    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(query, ())
        res = cur.fetchall()
        if res and res[0]:
            return "i" + res[0][0]


def _get_row_id(synset: Synset) -> int:
    get_query = """SELECT ss.rowid FROM synsets AS ss WHERE ss.id=?"""
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(get_query, (synset.id,))
        res = cur.fetchall()
        return res[0][0]


def _remove_relation(
    synset_source: Synset, synset_target: Synset, relationType: RelationType | int
) -> None:
    if isinstance(relationType, RelationType):
        relationType = relationType.value
    query = """
    
        DELETE FROM synset_relations WHERE lexicon_rowid = ? AND source_rowid = ? AND target_rowid = ? 
        and type_rowid = ?
    
    """
    data = (
        _get_row_id_from_lex(synset_source.lexicon().id),
        _get_row_id(synset_source),
        _get_row_id(synset_target),
        relationType,
    )
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()


def _set_relation_to_sense(
    sense: wn.Sense,
    synset: Synset,
    relationType: RelationType | int,
    meta: Optional[Metadata] = None,
):
    if isinstance(relationType, RelationType):
        relationType = relationType.value
    query = """
    
    INSERT INTO sense_synset_relations VALUES (null,?,?,?,?,?)
    
    """
    lex_rowid = get_row_id(
        "lexicons", {"id": sense.lexicon().id, "version": sense.lexicon().version}
    )
    rowid = get_row_id("senses", {"id": sense.id, "lexicon_rowid": lex_rowid})
    data = (
        _get_row_id_from_lex(sense.synset().lexicon().id),
        rowid,
        _get_row_id(synset),
        relationType,
        meta,
    )
    with connect() as conn:
        conn.cursor().execute(query, data)
        conn.commit()


def _set_relation_to_synset(
    synset_source: Synset,
    synset_target: Synset,
    relationType: RelationType | int,
    meta: Optional[Metadata] = None,
) -> None:
    if isinstance(relationType, RelationType):
        relationType = relationType.value
    query = """
        INSERT INTO synset_relations
        VALUES (null,?,?,?,?,?)
    """
    data = (
        _get_row_id_from_lex(synset_source.lexicon().id),
        _get_row_id(synset_source),
        _get_row_id(synset_target),
        relationType,
        meta,
    )
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()


class _Editor:
    """

    Abstract class used by all editors. This provides the :method:`set_modified` method which sets the correct
    Lexicon to be modified.


    """

    def __init__(self, lex_rowid):
        self.lex_rowid = lex_rowid

    def set_modified(self):
        with connect() as conn:
            if isinstance(self.lex_rowid, list):
                for rowid in self.lex_rowid:
                    conn.cursor().execute(SET_MOD_QUERY, (rowid,))
            else:
                conn.cursor().execute(SET_MOD_QUERY, (self.lex_rowid,))
            conn.commit()

    def get_lexicon_editor(self) -> Optional[LexiconEditor]:
        """
        Get the :class:`LexiconEditor` corresponding to this wn_editor.
        """
        if not isinstance(self.lex_rowid, list):
            return LexiconEditor(self.lex_rowid)


class LexiconEditor(_Editor):
    """

    The Lexicon Editor is the top most and most general wn_editor. It provides methods that can be applied to change
    a lexicon. This class can either be created with a row id of a lexicon or an id.

    >>> LexiconEditor("odenet")
    <wn.wn_editor.LexiconEditor object at 0x1023fb6a0>
    >>> LexiconEditor(1)
    <wn.wn_editor.LexiconEditor object at 0x1023fb430>

    """

    @classmethod
    def create_new_lexicon(
        cls,
        lex_id: str,
        label: str,
        language: str,
        email: str,
        lex_license: str,
        version: str,
        url: str = None,
        citation: str = None,
        logo: str = None,
        metadata: Optional[Metadata] = None,
    ) -> LexiconEditor:
        """
        Creates a new Lexicon with the attribute 'artificial' and returns its :class:`LexiconEditor`
        """
        query = """
        
        INSERT INTO lexicons VALUES (null,?,?,?,?,?,?,?,?,?,?,0)
        
        """
        metadata = metadata if metadata else {}
        if "note" in metadata:
            metadata["note"] = (
                metadata["note"] if metadata["note"] else ""
            ) + " _.artificial"
        else:
            metadata["note"] = " _.artificial"
        with connect() as conn:
            data = (
                lex_id,
                label,
                language,
                email,
                lex_license,
                version,
                url,
                citation,
                logo,
                metadata,
            )
            conn.cursor().execute(query, data)
            conn.commit()
            return LexiconEditor(
                get_row_id("lexicons", {"id": lex_id, "version": version})
            )

    @overload
    def __init__(self, lex_row_id: int) -> None:
        ...

    @overload
    def __init__(self, lex_id: str) -> None:
        ...

    def __init__(self, lex: str | int) -> None:
        if isinstance(lex, int):
            super(LexiconEditor, self).__init__(lex)
        else:
            super(LexiconEditor, self).__init__(_get_row_id_from_lex(lex))

    @_modifies_db
    def _id(self, lex_id: str):
        query = """
        
        UPDATE lexicons SET id = ? WHERE rowid = ?
        
        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (lex_id, self.lex_rowid))
            conn.commit()

    def create_synset(self) -> SynsetEditor:
        """
        Create a new Synset and return the wn_editor
        """
        return SynsetEditor(_get_lex_name_from_lex_id(self.lex_rowid))

    def create_sense(
        self, synset: Optional[Synset] = None, entry_row_id: Optional[int] = None
    ) -> SenseEditor:
        """
        Create a new sense and return the wn_editor
        Pass a Synset or/and an Entry id to automatically attach to those. Else they are created.
        """
        entry_edit = EntryEditor(entry_row_id) if entry_row_id else self.create_entry()
        synset_edit = SynsetEditor(synset) if synset else self.create_synset()
        return SenseEditor(self.lex_rowid, entry_edit.entry_id, synset_edit.rowid)

    def create_entry(self) -> EntryEditor:
        """
        Create a new entry and return the wn_editor
        """
        return EntryEditor(self.lex_rowid, False)

    def create_form(self, entry_row_id: Optional[int] = None) -> FormEditor:
        """
        Create a new form and return the wn_editor
        """
        return (
            FormEditor(entry_row_id)
            if entry_row_id
            else FormEditor(self.create_entry().entry_id)
        )

    def add_syntactic_behaviour(
        self, syn_id: str, frame: str, sense: Optional[wn.Sense] = None
    ):
        """
        Create a new Syntactic Behaviour. Can be passed a Sense to map it to
        """
        query = """
        INSERT INTO syntactic_behaviours VALUES (null,?,?,?)
        """
        with connect() as conn:
            conn.cursor().execute(query, (syn_id, self.lex_rowid, frame)).fetchall()
            rowid = get_row_id(
                "syntactic_behaviours", {"lexicon_rowid": self.lex_rowid, "id": syn_id}
            )
            SenseEditor(sense).add_syntactic_behaviour(rowid)
            conn.commit()

    def delete_syntactic_behaviour(
        self,
        syn_row_id: int = None,
        syn_id: str = None,
        frame: str = None,
    ):
        """
        Delete a syntactic behaviour
        """
        if (
            syn_row_id is None
            and None in (syn_id, frame)
            or syn_row_id is not None
            and (syn_id is not None or frame is not None)
        ):
            raise AttributeError
        else:
            if syn_row_id is not None:
                query = """
                DELETE from syntactic_behaviours WHERE rowid = ?
                """
                with connect() as conn:
                    conn.cursor().execute(query, (syn_row_id,))
                    conn.commit()
            else:
                query = """
                DELETE from syntactic_behaviours WHERE id = ? and lexicon_rowid = ? and frame = ?
                """
                with connect() as conn:
                    conn.cursor().execute(query, (syn_id, self.lex_rowid, frame))

    def as_lexicon(self) -> wn.Lexicon:
        return wn.lexicons(lexicon=_get_lex_name_from_lex_id(self.lex_rowid))[0]


class IlIEditor(_Editor):
    """

    The ILIEditor can be used to modify properties of IlIs inside the Database.
    It can be created using :class:`wn.Ili` a rowid or an id of an existing ili.
    If no argument is passed a new ILI will be created.

    """

    @overload
    def __init__(self, ili: wn.ILI):
        ...

    @overload
    def __init__(self, ili_row_id: int):
        ...

    @overload
    def __init__(self, ili_id: str):
        ...

    def __init__(self, ili: wn.ILI | int | str | None) -> None:
        super(IlIEditor, self).__init__(
            _get_all_lexicon_row_ids()
        )  # Modifies all lexicons.
        if isinstance(ili, wn.ILI):
            self.row_id = get_row_id("ilis", {"id": ili.id})
        elif isinstance(ili, int):
            self.row_id = ili
        elif isinstance(ili, str):
            self.row_id = get_row_id("ilis", {"id": ili})
        elif ili is None:
            self.row_id = self._create()

    @_modifies_db
    def _create(self) -> int:
        ili_id = _get_valid_ili_id()
        query = """
        
        INSERT INTO ilis VALUES (null,?,3,null,null)
        
        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (ili_id,))
            return get_row_id("ilis", {"id": ili_id})

    @_modifies_db
    def set_definition(self, definition: str):
        """

        Sets the definition of the ILI

        """
        query = """
        
        UPDATE ilis SET definition = ? WHERE rowid = ?
        
        """
        with connect() as conn:
            conn.cursor().execute(query, (definition, self.row_id))
            conn.commit()

    @_modifies_db
    def set_status(self, status: IliStatus):
        """

        Sets the status of the IlI

        """
        query = """
        UPDATE ilis SET status_rowid = ? WHERE rowid = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (status.value, self.row_id))
            conn.commit()

    @_modifies_db
    def set_meta(self, meta: Metadata):
        """

        Sets the metadata of the ILI

        """
        query = """
        UPDATE ilis SET metadata = ? WHERE rowid = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (meta, self.row_id))
            conn.commit()

    def as_ili(self) -> wn.ILI:
        """

        returns the :class:`wn.Ili` object.

        """
        query = """
        SELECT id from ilis WHERE rowid = ?
        """
        with connect() as conn:
            res = conn.cursor().execute(query, (self.row_id,)).fetchall()
            if res and res[0]:
                return wn.ili(res[0][0])


def _delete_relaton_to_sense(
    sense: wn.Sense, synset: Synset, reltype: RelationType | int
):
    if isinstance(reltype, RelationType):
        reltype = reltype.value
    query = """
    
    DELETE FROM sense_synset_relations WHERE lexicon_rowid = ? and source_rowid = ? and target_rowid = ? and 
    type_rowid = ?    
    """
    data = (
        _get_row_id_from_lex(sense.lexicon().id),
        SenseEditor(sense).row_id,
        SynsetEditor(synset).rowid,
        reltype,
    )
    with connect() as conn:
        conn.cursor().execute(query, data)
        conn.commit()


def _delete_sense_sense_relation(
    sense_source: wn.Sense, sense_target: wn.Sense, relation_type: RelationType | int
):
    if isinstance(relation_type, RelationType):
        relation_type = relation_type.value
    query = """
    
    DELETE FROM sense_relations WHERE lexicon_rowid = ? and source_rowid = ? and target_rowid = ? and type_rowid = ? 
    
    """
    data = (
        _get_row_id_from_lex(sense_source.lexicon().id),
        SenseEditor(sense_source).row_id,
        SenseEditor(sense_target).row_id,
        relation_type,
    )
    with connect() as conn:
        conn.execute(query, data)
        conn.commit()


class SynsetEditor(_Editor):
    """

    The SynsetEditor provides an wn_editor for a synset. It can be created with a :class:`wn.Synset`
    or a rowid / id of a lexicon. If a id is passed a new synset will be created.

    """

    @classmethod
    def from_rowid(cls, rowid: int):
        query = """
            SELECT synsets.id , l.id FROM synsets join lexicons l on synsets.lexicon_rowid = l.rowid 
            WHERE synsets.rowid = ?
            """
        with connect() as conn:
            res = conn.cursor().execute(query, (rowid,)).fetchall()
            if res is not None and res[0] is not None:
                return cls(wn.synset(id=res[0][0], lexicon=res[0][1]))

    @overload
    def __init__(self, synset: Synset) -> None:
        ...

    @overload
    def __init__(self, rowid: int) -> None:
        ...

    @overload
    def __init__(self, lexicon: str) -> None:
        ...

    def __init__(self, inp: Synset | int | str) -> None:

        if isinstance(inp, Synset):
            lex_rowid = get_row_id(
                "lexicons", {"id": inp.lexicon().id, "version": inp.lexicon().version}
            )
            super().__init__(lex_rowid)
            self.rowid = get_row_id(
                "synsets", {"id": inp.id, "lexicon_rowid": self.lex_rowid}
            )
        elif isinstance(inp, int):
            super().__init__(inp)
            self.exists = False
            self.rowid = self._create(_get_valid_synset_id(self.lex_rowid) + "-u", None)
        else:
            super().__init__(_get_row_id_from_lex(inp))
            self.rowid: int = self._create(
                _get_valid_synset_id(self.lex_rowid) + "-u", None
            )

    @_modifies_db
    def _create(self, syn_id, meta) -> int:
        query = """
        INSERT INTO synsets VALUES (null,?,?,null,null,1,null,?)
        """
        data = (syn_id, self.lex_rowid, meta)
        with connect() as conn:
            conn.cursor().execute(query, data)
            conn.commit()
            return get_row_id(
                "synsets", {"id": syn_id, "lexicon_rowid": self.lex_rowid}
            )

    @_modifies_db
    def add_word(self, word: str) -> SynsetEditor:
        """

        This is a shortcut method to create a new word/entry inside the synset. This method will create an entry and
        a sense, assign fitting ids and add a form to the sense which contains the argument as word.

        """
        entry_edit = EntryEditor(self.lex_rowid, exists=False)
        SenseEditor(
            lexicon_rowid=self.lex_rowid,
            entry_rowid=entry_edit.entry_id,
            synset_rowid=self.rowid,
        ).set_id(_get_valid_sense_id(entry_id=entry_edit.entry_id, form=word))
        entry_edit.add_form(word)
        return self

    @_modifies_db
    def delete_word(self, word: str) -> SynsetEditor:
        """

        This method will try to delete a word from a synset, by searching trough all senses' words until it findes a
        wordwhich as the argument as lemma. Then the sense is deleted.
        Warning: this deletes the __Sense__ not the relation.

        """
        for w in self.as_synset().words():
            if w.lemma() == word:
                for se in w.senses():
                    SenseEditor(se).delete()
        return self

    def set_hypernym_of(self, synset: Synset | str) -> SynsetEditor:
        """
        Shortcut method to make this synset a hypernym of another synset.
        Can be called with a string to automatically create a new synset and add the argument to it.
        """
        self.set_relation_to_synset(synset, RelationType.hypernym)
        return self

    def set_hyponym_of(self, synset: Synset | str) -> SynsetEditor:
        """
        Shortcut method to make this synset a hypernym of another synset.
        Can be called with a string to automatically create a new synset and add the argument to it.
        """
        self.set_relation_to_synset(synset, RelationType.hyponym)
        return self

    def set_holonym_member_of(self, synset: Synset | str) -> SynsetEditor:
        """
        Shortcut method to make this synset a hypernym of another synset.
        Can be called with a string to automatically create a new synset and add the argument to it.
        """
        self.set_relation_to_synset(synset, RelationType.holo_member)
        return self

    def set_holonym_part_of(self, synset: Synset | str) -> SynsetEditor:
        """
        Shortcut method to make this synset a hypernym of another synset.
        Can be called with a string to automatically create a new synset and add the argument to it.
        """
        self.set_relation_to_synset(synset, RelationType.holo_part)
        return self

    @_modifies_db
    def set_relation_to_sense(
        self, sense: wn.Sense, relation_type: RelationType | int
    ) -> SynsetEditor:
        """

        Sets the relation between a sense and a synset.

        """
        _set_relation_to_sense(sense, self.as_synset(), relation_type)
        return self

    @_modifies_db
    def delete_relation_to_sense(
        self, sense: wn.Sense, relation_type: RelationType | int
    ):
        """
        Deletes a relation to a sense
        """
        _delete_relaton_to_sense(sense, self.as_synset(), relation_type)
        return self

    @_modifies_db
    def set_relation_to_synset(
        self, synset: Synset | str, relation_type: RelationType | int
    ) -> SynsetEditor:
        """

        This method sets a relation to another synset. It can also be called with a string to automatically create a
        new synset to set the relation to.

        """
        if not isinstance(synset, Synset):
            synset = SynsetEditor(self.lex_rowid).add_word(synset).as_synset()
        _set_relation_to_synset(synset, self.as_synset(), relation_type)
        return self

    @_modifies_db
    def delete_relation_to_synset(
        self, synset: Synset | str, reltype: RelationType | int
    ) -> SynsetEditor:
        """

        This method deletes a relation to a synset.
        It can also be called with a string to delete the relation to all synsets which contain this word.
        (Potentially unsafe)


        """
        if isinstance(synset, str):
            logger.warn(
                f"Removing relation to ALL synsets wn can find with name '{synset}'"
            )
            for sset in wn.synsets(synset):
                _remove_relation(sset, self.as_synset(), reltype)
        else:
            _remove_relation(synset, self.as_synset(), reltype)
        return self

    @_modifies_db
    def delete(self) -> None:
        """

        Deletes this synset from the database

        """
        query = """
        DELETE FROM synsets WHERE rowid = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.rowid,))
            conn.commit()

    @_modifies_db
    def set_ili(self, ili: int | wn.ILI) -> SynsetEditor:
        """

        Sets the ILI of this Synset. Takes a rowid or a :class:`wn.ILI`.


        """
        query = """
        
        UPDATE synsets SET ili_rowid = ? WHERE rowid = ?
        
        """
        if isinstance(ili, wn.ILI):
            ili = IlIEditor(ili).row_id
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (ili, self.rowid))
            conn.commit()
        return self

    @_modifies_db
    def delete_ili(self) -> SynsetEditor:
        """

        Removes the ILI from the Synset

        """
        query = """
        
        UPDATE synsets SET ili_rowid = null WHERE rowid = ?
        
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.rowid,))
            conn.commit()
        return self

    def as_synset(self) -> wn.Synset:
        query = """
        SELECT synsets.id , l.id FROM synsets join lexicons l on synsets.lexicon_rowid = l.rowid WHERE synsets.rowid = ?
        """
        with connect() as conn:
            res = conn.cursor().execute(query, (self.rowid,)).fetchall()
            if res is not None and res[0] is not None:
                return wn.synset(id=res[0][0], lexicon=res[0][1])

    @_modifies_db
    def add_definition(
        self,
        definition: str,
        sense: Optional[wn.Sense] = None,
        language: Optional[str] = None,
        metadata: Optional[Metadata] = None,
    ) -> SynsetEditor:
        """

        Add a definition

        """
        query = """ 
        INSERT INTO definitions VALUES (null,?,?,?,?,?,?)
        """
        data = (
            self.lex_rowid,
            self.rowid,
            definition,
            language,
            SenseEditor(sense).row_id if sense else None,
            metadata,
        )
        with connect() as conn:
            conn.cursor().execute(query, data)
            conn.commit()
        return self

    @_modifies_db
    def add_example(
        self,
        example: str,
        language: Optional[str] = None,
        meta: Optional[Metadata] = None,
    ) -> SynsetEditor:
        """
        Add an example to this synset
        """
        query = """
        
        INSERT INTO synset_examples VALUES ( null,?,?,?,?,?)
        """
        with connect() as conn:
            conn.cursor().execute(
                query,
                (
                    self.lex_rowid,
                    self.rowid,
                    example,
                    language,
                    meta,
                ),
            )
            conn.commit()
        return self

    @_modifies_db
    def delete_example(self, example) -> SynsetEditor:
        """
        Delete an example from this synset
        """
        query = """
        
        DELETE FROM synset_examples WHERE lexicon_rowid = ? and synset_rowid = ? and example = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.lex_rowid, self.rowid, example))
            conn.commit()
        return self

    @_modifies_db
    def set_proposed_ili(
        self, definition: str, meta: Optional[Metadata] = None
    ) -> SynsetEditor:
        """
        Set the proposed ILI
        """
        exists_query = """
        
        SELECT EXISTS(SELECT 1 FROM proposed_ilis WHERE synset_rowid = ?)
        
        """
        if bool(
            connect().cursor().execute(exists_query, (self.rowid,)).fetchall()[0][0]
        ):
            # Exists -> Modify
            query = (
                """
            UPDATE proposed_ilis SET definition = ? WHERE synset_rowid = ?
            """
                if not meta
                else """
            UPDATE proposed_ilis SET definition = ? , metadata = ? WHERE synset_rowid = ?
            """
            )
            with connect() as conn:
                conn.cursor().execute(
                    query,
                    (definition, self.rowid)
                    if not meta
                    else (definition, meta, self.rowid),
                )
                conn.commit()
        else:
            query = """
            INSERT INTO proposed_ilis VALUES (null,?,?,?)
            """
            with connect() as conn:
                conn.cursor().execute(query, (self.rowid, definition, meta))
                conn.commit()
        return self

    @_modifies_db
    def delete_proposed_ili(self) -> SynsetEditor:
        """
        Delete the Proposed ILI
        """
        query = """
        DELETE FROM proposed_ilis WHERE synset_rowid = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.rowid,))
            conn.commit()
        return self


def _get_sense_info_from_row_id(rowid: int) -> tuple[int, int, int, str]:
    query = """
    
    SELECT lexicon_rowid,entry_rowid,synset_rowid,id FROM senses WHERE rowid = ?
    
    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(query, (rowid,))
        res = cur.fetchall()
        if res and res[0]:
            return int(res[0][0]), int(res[0][1]), int(res[0][2]), str(res[0][3])


def _set_sense_sense_relation(
    sense_source: wn.Sense,
    sense_target: wn.Sense,
    relation_type: RelationType | int,
    meta: Optional[Metadata] = None,
):
    query = """
    
    INSERT INTO sense_relations VALUES (null,?,?,?,?,?)
    
    
    """
    if isinstance(relation_type, RelationType):
        relation_type = relation_type.value
    data = (
        _get_row_id_from_lex(sense_source.lexicon().id),
        SenseEditor(sense_source).row_id,
        SenseEditor(sense_target).row_id,
        relation_type,
        meta,
    )
    with connect() as conn:
        conn.cursor().execute(query, data)
        conn.commit()


class SenseEditor(_Editor):
    """

    The SenseEditor provides an wn_editor for :class:`wn.Sense`. It can be created using a existing :class:`wn.Sense` or
    passing the lexicon_rowid, the entry_rowid and the synset_rowid as arguments.

    """

    def __init__(
        self,
        sense: wn.Sense = None,
        lexicon_rowid: int = None,
        entry_rowid: int = None,
        synset_rowid: int = None,
    ):
        if not (
            (sense and not (lexicon_rowid or entry_rowid or synset_rowid))
            or (not sense and (lexicon_rowid and entry_rowid and synset_rowid))
        ):
            raise AttributeError
        else:
            if sense:
                lex_row = get_row_id(
                    "lexicons",
                    {"id": sense.lexicon().id, "version": sense.lexicon().version},
                )
                self.row_id = get_row_id(
                    "senses", {"id": sense.id, "lexicon_rowid": lex_row}
                )
                lex_id, self.entry_id, self.synset_id, _ = _get_sense_info_from_row_id(
                    self.row_id
                )
                super(SenseEditor, self).__init__(lex_id)
            else:
                super(SenseEditor, self).__init__(lexicon_rowid)
                self.entry_id = entry_rowid
                self.synset_id = synset_rowid
                self.row_id = self._create()

    @_modifies_db
    def _create(self) -> int:
        query = """
        INSERT INTO senses VALUES(null,?,?,?,null,?,null,1,null)
        """
        with connect() as conn:
            cur = conn.cursor()
            new_id = _get_valid_sense_id(self.entry_id)
            data = (
                new_id,
                self.lex_rowid,
                self.entry_id,
                self.synset_id,
            )
            cur.execute(query, data)
            return get_row_id("senses", {"id": new_id, "lexicon_rowid": self.lex_rowid})

    @_modifies_db
    def set_id(self, new_id: str) -> SenseEditor:
        """
        Sets the ID of the Sense
        """
        query = """
        UPDATE senses SET id = ? WHERE rowid = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (new_id, self.row_id))
            conn.commit()
            return self

    @_modifies_db
    def delete(self) -> None:
        """

        Deletes the sense from the database

        """
        query = """
        
        DELETE FROM senses WHERE rowid = ?
        
        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (self.row_id,))
            conn.commit()

    def as_sense(self) -> wn.Sense:
        """

        Returns the corresponding :class:`wn.Sense` object

        """
        _, _, _, sense_id = _get_sense_info_from_row_id(self.row_id)
        return wn.sense(sense_id)

    @_modifies_db
    def set_relation_to_synset(self, synset: Synset, relation_type: RelationType):
        """

        Sets the relation of this Sense to a synset.

        """
        s = self.as_sense()
        _set_relation_to_sense(s, synset, relation_type)
        return self

    @_modifies_db
    def delete_relation_to_synset(self, synset: Synset, relation_type: RelationType):
        """

        Deletes a relation of this sense to a synset.

        """
        s = self.as_sense()
        _delete_relaton_to_sense(s, synset, relation_type)
        return self

    @_modifies_db
    def set_relation_to_sense(self, sense: wn.Sense, relation_type: RelationType):
        """

        Sets the relation of this sense to another sense.

        """
        s = self.as_sense()
        _set_sense_sense_relation(s, sense, relation_type)
        return self

    @_modifies_db
    def delete_relation_to_sense(self, sense: wn.Sense, relation_type: RelationType):
        """

        Deletes the relation of this sense to another sense

        """
        s = self.as_sense()
        _delete_sense_sense_relation(s, sense, relation_type)
        return self

    @_modifies_db
    def add_adjposition(self, adjposition: str) -> SenseEditor:
        """

        Add an adjposition to the sense.

        """
        query = """
        
        INSERT INTO adjpositions VALUES (?,?)
        
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.row_id, adjposition))
            conn.commit()
        return self

    @_modifies_db
    def delete_adjposition(self, adjposition) -> SenseEditor:
        """
        Deletes an adjposition of the sense
        """
        query = """
        DELETE from adjpositions WHERE sense_rowid = ? and adjposition = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, adjposition)
            conn.commit()
        return self

    def _count_exists(self) -> bool:
        query = """
        SELECT exists(SELECT 1 FROM counts WHERE sense_rowid=? and lexicon_rowid =?)
        """
        with connect() as conn:
            return bool(
                conn.cursor()
                .execute(query, (self.row_id, self.lex_rowid))
                .fetchall()[0][0]
            )

    @_modifies_db
    def set_count(
        self, count: int | wn.Count, meta: Optional[Metadata] = None
    ) -> SenseEditor:
        """
        set the count of the sense
        """
        query = """
            INSERT INTO counts VALUES (null,?,?,?,?)
            """
        with connect() as conn:
            conn.execute(query, (self.lex_rowid, self.row_id, count, meta))
            conn.commit()
        return self

    @_modifies_db
    def delete_count(self, count: wn.Count | int):
        """
        Delete the count of the synset
        """
        query = """
        
        DELETE FROM counts WHERE sense_rowid = ? and lexicon_rowid = ? and count = ?
        
        """
        with connect() as conn:
            conn.execute(query, (self.row_id, self.lex_rowid, count))
            conn.commit()

    @_modifies_db
    def update_count(
        self,
        count: wn.Count,
        new_count: wn.Count | int,
        meta: Optional[Metadata] = None,
    ):
        """
        Updates an existing count
        """

        query = (
            """
        
        UPDATE counts SET count = ? WHERE count = ? and sense_rowid = ? and lexicon_rowid = ? 
        
        """
            if not meta
            else """
        UPDATE counts SET count = ? , metadata = ? WHERE count = ? and sense_rowid = ? and lexicon_rowid = ?
        """
        )
        with connect() as conn:
            conn.cursor().execute(
                query,
                (new_count, count, self.row_id, self.lex_rowid)
                if not meta
                else (new_count, meta, count, self.row_id, self.lex_rowid),
            )

    @_modifies_db
    def add_example(
        self,
        example: str,
        language: Optional[str] = None,
        meta: Optional[Metadata] = None,
    ) -> SenseEditor:
        """
        Add an example to this sense
        """
        query = """
        INSERT INTO sense_examples VALUES (null,?,?,?,?,?)
        """
        with connect() as conn:
            conn.cursor().execute(
                query, (self.lex_rowid, self.row_id, example, language, meta)
            )
            conn.commit()
        return self

    @_modifies_db
    def delete_example(self, example: str) -> SenseEditor:
        """
        Remove an example from this Sense
        """
        query = """
        DELETE FROM sense_examples WHERE example = ? and lexicon_rowid = ? and sense_rowid = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (example, self.lex_rowid, self.row_id))
            conn.commit()
        return self

    @_modifies_db
    def add_syntactic_behaviour(self, syn_id: int):
        """
        Add Syntactic Behaviour
        """
        query = """

        INSERT INTO syntactic_behaviour_senses VALUES(?,?)

        """
        with connect() as conn:
            conn.cursor().execute(query, (syn_id, self.row_id))

    @_modifies_db
    def delete_syntactic_behaviour(self, syn_id: int):
        """
        Delete a Syntactic Behaviour
        """
        query = """
        DELETE FROM syntactic_behaviour_senses WHERE sense_rowid = ? and syntactic_behaviour_rowid = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.row_id, syn_id))
            conn.commit()


class EntryEditor(_Editor):
    """

    This Editor can be used to edit Entries. Since there exists no class which captures those, it can only be created
    using an entry_id or passing a lexicon_rowid to create a new one.

    """

    _get_lex_id_from_entry_query = """
        SELECT lexicon_rowid from entries WHERE rowid = ?
    """

    def __init__(self, m_id: int, exists: bool = True):
        if exists:
            # exists
            self.entry_id = m_id
            super(EntryEditor, self).__init__(self._get_lex_id_from_entry(m_id))
        else:
            super(EntryEditor, self).__init__(m_id)
            self.entry_id = self._create()

    @_modifies_db
    def _create(self) -> int:
        query = """
        INSERT INTO entries VALUES (null,?,?,?,null) 
        """
        en_id = _get_valid_entity_id()
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (en_id, self.lex_rowid, "u"))
            return get_row_id("entries", {"id": en_id, "lexicon_rowid": self.lex_rowid})

    @_modifies_db
    def set_pos(self, pos: str):
        """

        sets the position of the entry

        """
        query = """
        
        UPDATE entries SET pos = ? WHERE rowid = ?
        
        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (pos, self.entry_id))
            conn.commit()
        return self

    def add_form(self, form, normalized_form=None):
        """

        shortcut function to add a form to this entry

        """
        editor = FormEditor(self.entry_id)
        editor.set_form(form)
        if normalized_form:
            editor.set_normalized_form(normalized_form)
        return self

    @_modifies_db
    def _set_id(self, new_id: str):
        query = """
        
        UPDATE entries SET id =? WHERE rowid = ?
        
        """
        with connect() as conn:
            conn.cursor().execute(query, (new_id, self.entry_id))
            conn.commit()
        return self

    def _get_id(self) -> str:
        query = """
        
        SELECT id from entries WHERE rowid = ?
        
        """
        res = connect().cursor().execute(query, (self.entry_id,)).fetchall()
        if res and res[0]:
            return str(res[0][0])

    def _get_lex_id_from_entry(self, entry_id) -> int:
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(self._get_lex_id_from_entry_query, (entry_id,))
            res = cur.fetchall()
            if res and res[0]:
                return int(res[0][0])

    @_modifies_db
    def delete(self) -> None:
        """

        Deletes this entry from the database

        """
        query = """
        
        DELETE from entries WHERE rowid = ?
        
        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (self.entry_id,))
            conn.commit()


class FormEditor(_Editor):
    """

    The FormEditor provides a wn_editor for :class:`wn.Form`. It can be created using an existing form or an entry_id

    """

    _query = """

    UPDATE forms SET %s = ? WHERE rowid = ? 

    """
    _get_lex_id_from_entry_query = """
    
        SELECT lexicon_rowid from entries WHERE rowid = ?
    """
    _get_lex_id_from_rowid_query = """
    
        SELECT lexicon_rowid from forms where rowid = ?
    
    """

    @overload
    def __init__(self, form: wn.Form) -> None:
        ...

    @overload
    def __init__(self, entry_id: int) -> None:
        ...

    def __init__(self, inp: int | wn.Form) -> None:
        if isinstance(inp, wn.Form):
            self.row_id = inp._id
            super(FormEditor, self).__init__(self._get_lex_id_from_rowid(self.row_id))
        if isinstance(inp, int):
            self.entry_id = inp
            super(FormEditor, self).__init__(self._get_lex_id_from_entry(self.entry_id))
            self.row_id = self._create()

    def _get_lex_id_from_rowid(self, row_id) -> int:
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(self._get_lex_id_from_rowid_query, (row_id,))
            res = cur.fetchall()
            if res and res[0]:
                return res[0][0]

    def _get_lex_id_from_entry(self, entry_id) -> int:
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(self._get_lex_id_from_entry_query, (entry_id,))
            res = cur.fetchall()
            if res and res[0]:
                return int(res[0][0])

    @_modifies_db
    def _create(self) -> int:
        query = """
        
        INSERT INTO forms VALUES (null,null,?,?,?,null,null,null) 
        
        """
        with connect() as conn:
            cur = conn.cursor()
            data = (self.lex_rowid, self.entry_id, "_")
            cur.execute(query, data)
            conn.commit()
            return get_row_id("forms", {"entry_rowid": self.entry_id, "form": "_"})

    @_modifies_db
    def set_form(self, form: str) -> FormEditor:
        """

        Sets the form filed of this form

        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(self._query % "form", (form, self.row_id))
        return self

    @_modifies_db
    def set_normalized_form(self, norm_form: str) -> FormEditor:
        """

        Sets the normalized form

        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(self._query % "normalized_form", (norm_form, self.row_id))
        return self

    @_modifies_db
    def _set_entry_rowid(self, rowid: int) -> FormEditor:
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(self._query % "entry_rowid", (rowid, self.row_id))
        return self

    @_modifies_db
    def _set_id(self, form_id: str) -> FormEditor:
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(self._query % "id", (form_id, self.row_id))
        return self

    @_modifies_db
    def delete(self) -> None:
        """

        Deletes the form from the database

        """
        query = """
        
        DELETE FROM forms WHERE rowid = ?
        
        """
        with connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (self.row_id,))
            conn.commit()

    @_modifies_db
    def add_pronunciation(
        self,
        pronunciation,
        variety: str = None,
        notation: str = None,
        phonemic: bool = True,
        audio: str = None,
    ):
        """

        Adds a pronunciation to the form

        """
        query = """
        
        INSERT INTO pronunciations VALUES (?,?,?,?,?,?)
        
        """
        with connect() as conn:
            data = (self.row_id, pronunciation, variety, notation, phonemic, audio)
            conn.cursor().execute(query, data)
            conn.commit()

    @_modifies_db
    def delete_pronunciation(
        self,
        pronunciation,
        variety: str = None,
        notation: str = None,
        phonemic: bool = True,
        audio: str = None,
    ):
        """

        Deletes a pronunciation from this form.
        Warning: This is potentially unsafe since there exists no primary key for pronunciations.

        """
        logger.warn("Deletion of pronunciations is potentially unsafe (no primary key)")
        query = """
        
        DELETE from pronunciations WHERE form_rowid = ? and value = ? and variety = ? and notation = ? and phonemic = ? 
        and audio = ?
        
        """
        with connect() as conn:
            data = (pronunciation, variety, notation, phonemic, audio)
            conn.cursor().execute(query, data)
            conn.commit()

    @_modifies_db
    def add_tag(self, tag: str, category: str) -> FormEditor:
        """
        Add a tag to this form
        """
        query = """
        INSERT INTO tags VALUES (?,?,?)
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.row_id, tag, category))
            conn.commit()
        return self

    @_modifies_db
    def delete_tag(self, tag: str, category: str) -> FormEditor:
        """
        Delete tag from this Form
        """
        query = """
        DELETE FROM tags WHERE form_rowid = ? and tag = ? and category = ?
        """
        with connect() as conn:
            conn.cursor().execute(query, (self.row_id, tag, category))
            conn.commit()
        return self
