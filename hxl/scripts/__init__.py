def parse_tags(s):
    '''Parse tags out from a comma-separated list'''
    def fix_tag(t):
        '''trim whitespace and add # if needed'''
        t = t.strip()
        if not t.startswith('#'):
            t = '#' + t
        return t
    return map(fix_tag, s.split(','))
