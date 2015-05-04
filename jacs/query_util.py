


def intersects(geometry_statement, geomety_field='geometry')
    # append the intersection to the where clause.
    if geometry_statement:
        return 'ST_Intersects(%s, %s)' % (geometry_field, geometry_statement)
