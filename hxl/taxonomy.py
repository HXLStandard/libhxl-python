"""
Taxonomy support for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
Started January 2015

License: Public Domain
Documentation: https://github.com/HXLStandard/libhxl-python/wiki
"""

from . import HXLException

class HXLTaxonomyException(HXLException):

    def __init__(self, message):
        super(HXLTaxonomyException, self).__init__(message)

class HXLTaxonomy:

    def __init__(self, terms=None):
        self.terms = terms if terms is not None else {}

    def add(self, term):
        """
        Add a new term
        @param term the new HXLTerm to add
        @return the old term with the same code, if any
        """
        old_term = self.terms.get(term.code)
        self.terms[term.code] = term
        return old_term

    def get(self, code):
        """
        Try getting an existing term
        @param code the code of the term
        @return the term, or None if not found
        """
        return self.terms.get(code)

    def contains(self, code, level=None):
        """
        Check if the taxonomy contains a term
        Will also match the level if provided
        @param code the term's code
        @param level the term's level (or None if not checked)
        @return True if found, False otherwise
        """
        term = self.get(code)
        return True if term and (level is None or int(term.level)==int(level)) else False

    def is_valid(self, callback=None):
        """
        Test whether a taxonomy is referentially-complete and loop-free.
        @param callback a function to call for each error, using the form callback(message, term)
        @return True if the taxonomy is valid; False if not
        """

        def contains_loop(code, ancestors=[]):
            """Test for loops"""
            if not code:
                # successful termination
                return False
            elif code in ancestors:
                # found a loop
                return True
            else:
                term = self.get(code)
                if term:
                    return contains_loop(term.parent_code, ancestors + [code])
                else:
                    return False

        result = True
        for code in self.terms:
            term = self.terms[code]
            if term.parent_code:
                # check for parent
                if not self.terms.get(term.parent_code):
                    if callback is not None:
                        callback("Parent does not exist", term)
                    result = False
                # check for loops
                if contains_loop(term.parent_code):
                    if callback is not None:
                        callback("Loop in ancestors", term)
                    result = False
        return result
        

class HXLTerm:

    __slots__ = ['code', 'parent_code', 'level']

    def __init__(self, code, parent_code=None, level=None):
        self.code = code
        self.parent_code = parent_code
        self.level = level


def readTaxonomy(source):
    """
    Read a taxonomy from a HXL data source.
    Throws a HXLTaxonomyException for duplicate terms.
    @param a HXLDataProvider
    @return a HXLTaxonomy
    """

    taxonomy = HXLTaxonomy()

    for row in source:
        # skip empty rows
        if len(row.values) == 0:
            continue

        # get the code
        code = row.get('#term_id')
        if code:
            parent_code = row.get('#parent_id')
            level = row.get('#level_num')
            term = HXLTerm(code, parent_code, level)
            old_term = taxonomy.add(term)
            if old_term is not None:
                raise HXLTaxonomyException("Duplicate term: " + old_term.code)
        else:
            raise HXLTaxonomyException("Missing #term_id in row " + str(row.sourceRowNumber))

    return taxonomy
                
            
